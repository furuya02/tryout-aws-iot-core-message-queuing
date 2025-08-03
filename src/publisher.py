#!/usr/bin/env python3
"""
AWS IoT Core Message Queuing テスト用パブリッシャー
QoS1メッセージを定期的に送信し、シェアサブスクライバーの応答を監視
"""

import json
import time
import uuid
import threading
from datetime import datetime
from typing import Optional
from awsiot import mqtt_connection_builder
from awscrt import mqtt, http
from concurrent.futures import Future
from .config import AWSIoTConfig


class IoTMessagePublisher:
    def __init__(self, config: AWSIoTConfig):
        self.config = config
        self.client_id = f"{config.client_id_prefix}-publisher"
        self.mqtt_connection: Optional[mqtt.Connection] = None
        self.is_connected = False
        self.publish_count = 0
        self.lock = threading.Lock()

    def setup_mqtt_connection(self) -> mqtt.Connection:
        """AWS IoT SDK MQTT接続のセットアップ"""
        return mqtt_connection_builder.mtls_from_path(
            endpoint=self.config.endpoint,
            cert_filepath=self.config.cert_path,
            pri_key_filepath=self.config.private_key_path,
            ca_filepath=self.config.root_ca_path,
            client_id=self.client_id,
            clean_session=False,
            keep_alive_secs=30,
            on_connection_interrupted=self._on_connection_interrupted,
            on_connection_resumed=self._on_connection_resumed
        )

    def _on_connection_interrupted(self, connection, error, **kwargs):
        """接続中断時のコールバック"""
        self.is_connected = False
        print(f"[Publisher] 接続中断: {self.client_id}, エラー: {error}")
        print("[Publisher] 自動再接続を待機中...")

    def _on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        """接続復旧時のコールバック"""
        self.is_connected = True
        print(f"[Publisher] 接続復旧: {self.client_id}, セッション保持: {session_present}")

    def _on_publish_complete(self, publish_future):
        """パブリッシュ完了時のコールバック"""
        try:
            publish_future.result()  # Exception が発生した場合はここで例外が発生
            with self.lock:
                self.publish_count += 1
                print(f"[Publisher] メッセージ送信完了 (総数: {self.publish_count})")
        except Exception as e:
            print(f"[Publisher] メッセージ送信失敗: {e}")

    def connect(self) -> bool:
        """AWS IoT Coreに接続"""
        try:
            self.mqtt_connection = self.setup_mqtt_connection()
            print(f"[Publisher] {self.config.endpoint} に接続中...")
            
            connect_future = self.mqtt_connection.connect()
            # 接続完了を待機
            connect_result = connect_future.result(timeout=10)
            
            self.is_connected = True
            session_present = connect_result.get('session_present', False) if isinstance(connect_result, dict) else getattr(connect_result, 'session_present', False)
            print(f"[Publisher] 接続成功: {self.client_id}, セッション保持: {session_present}")
            return True
            
        except Exception as e:
            print(f"[Publisher] 接続エラー: {e}")
            self.is_connected = False
            return False

    def publish_test_message(self, message_id: Optional[str] = None) -> bool:
        """テストメッセージを送信"""
        if not self.is_connected or not self.mqtt_connection:
            print("[Publisher] 未接続のためメッセージ送信をスキップ")
            return False

        if message_id is None:
            message_id = str(uuid.uuid4())

        message = {
            "message_id": message_id,
            "timestamp": datetime.now().isoformat(),
            "sender": self.client_id,
            "data": {"temperature": 25.5, "humidity": 60.0, "status": "normal"},
            "sequence": self.publish_count + 1,
        }

        try:
            topic = self.config.get_publish_topic()
            publish_future, packet_id = self.mqtt_connection.publish(
                topic=topic,
                payload=json.dumps(message),
                qos=mqtt.QoS.AT_LEAST_ONCE
            )
            
            # 非同期でパブリッシュ完了を待機
            publish_future.add_done_callback(self._on_publish_complete)
            
            print(f"[Publisher] メッセージ送信: {message_id} (Packet ID: {packet_id})")
            return True
            
        except Exception as e:
            print(f"[Publisher] 送信エラー: {e}")
            return False

    def start_continuous_publishing(
        self, interval: float = 2.0, max_messages: int = 50
    ):
        """連続的にメッセージを送信"""
        print(f"[Publisher] 連続送信開始 (間隔: {interval}秒, 最大: {max_messages}件)")

        for i in range(max_messages):
            if not self.is_connected:
                print("[Publisher] 接続が切断されたため送信を停止")
                break

            success = self.publish_test_message()
            if success:
                print(f"[Publisher] 進捗: {i+1}/{max_messages}")

            time.sleep(interval)

        print("[Publisher] 連続送信完了")
        print(f"📊 送信サマリー: {self.publish_count}件のメッセージを送信しました")
        print("📨 サブスクライバーでメッセージ受信を確認してください")

    def disconnect(self):
        """接続を切断"""
        if self.mqtt_connection and self.is_connected:
            try:
                disconnect_future = self.mqtt_connection.disconnect()
                disconnect_future.result(timeout=10)
                self.is_connected = False
                print("[Publisher] 切断完了")
            except Exception as e:
                # 既に切断されている場合のエラーは無視
                if "NOT_CONNECTED" in str(e):
                    print("[Publisher] 既に切断済み")
                else:
                    print(f"[Publisher] 切断エラー: {e}")
                self.is_connected = False
        elif not self.is_connected:
            print("[Publisher] 既に切断状態です")


def main():
    """メイン実行関数"""
    config = AWSIoTConfig()

    if not config.validate():
        print("設定の検証に失敗しました")
        return

    publisher = IoTMessagePublisher(config)

    try:
        # 接続
        if not publisher.connect():
            print("接続に失敗しました")
            return

        # テストメッセージの送信
        print("\n=== メッセージキューイングテスト開始 ===")
        publisher.start_continuous_publishing(interval=1.0, max_messages=20)

        # 少し待機
        time.sleep(5)

    except KeyboardInterrupt:
        print("\n[Publisher] 中断されました")
    finally:
        publisher.disconnect()


if __name__ == "__main__":
    main()
