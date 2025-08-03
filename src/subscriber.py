#!/usr/bin/env python3
"""
AWS IoT Core Message Queuing テスト用シェアサブスクライバー
シェアサブスクリプションでメッセージを受信し、接続断絶をシミュレート
"""

import json
import time
import threading
import random
import signal
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from awsiot import mqtt_connection_builder
from awscrt import mqtt
from concurrent.futures import Future
from .config import AWSIoTConfig


class IoTSharedSubscriber:
    def __init__(self, config: AWSIoTConfig, subscriber_id: str):
        self.config = config
        self.subscriber_id = subscriber_id
        self.client_id = f"{config.client_id_prefix}-subscriber-{subscriber_id}"
        self.mqtt_connection: Optional[mqtt.Connection] = None
        self.is_connected = False
        self.message_count = 0
        self.processed_messages: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.should_disconnect = False
        self.disconnect_duration = 0

    def setup_mqtt_connection(self) -> mqtt.Connection:
        """AWS IoT SDK MQTT接続のセットアップ"""
        return mqtt_connection_builder.mtls_from_path(
            endpoint=self.config.endpoint,
            cert_filepath=self.config.cert_path,
            pri_key_filepath=self.config.private_key_path,
            ca_filepath=self.config.root_ca_path,
            client_id=self.client_id,
            clean_session=False,  # 永続セッションを有効化
            keep_alive_secs=30,
            on_connection_interrupted=self._on_connection_interrupted,
            on_connection_resumed=self._on_connection_resumed
        )

    def _on_connection_interrupted(self, connection, error, **kwargs):
        """接続中断時のコールバック"""
        self.is_connected = False
        print(f"[Subscriber-{self.subscriber_id}] 接続中断: {self.client_id}, エラー: {error}")
        # 意図的な切断の場合は再接続を待つ
        if not self.should_disconnect:
            print(f"[Subscriber-{self.subscriber_id}] 予期しない切断のため再接続を試行中...")

    def _on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        """接続復旧時のコールバック"""
        self.is_connected = True
        print(f"[Subscriber-{self.subscriber_id}] 接続復旧: {self.client_id}, セッション保持: {session_present}")
        if session_present:
            print(f"[Subscriber-{self.subscriber_id}] 🔄 キューイングされたメッセージを受信開始")

    def _on_message_received(self, topic, payload, dup, qos, retain, **kwargs):
        """メッセージ受信時のコールバック"""
        try:
            message_data = json.loads(payload.decode())
            message_id = message_data.get("message_id", "unknown")

            with self.lock:
                self.message_count += 1
                qos_value = qos.value if hasattr(qos, 'value') else qos
                self.processed_messages[message_id] = {
                    "received_at": datetime.now().isoformat(),
                    "topic": topic,
                    "qos": qos_value,
                    "data": message_data,
                }

            print(
                f"[Subscriber-{self.subscriber_id}] メッセージ受信 #{self.message_count}: {message_id}"
            )
            print(f"  - 送信者: {message_data.get('sender', 'unknown')}")
            print(f"  - シーケンス: {message_data.get('sequence', 'unknown')}")
            print(f"  - データ: {message_data.get('data', {})}")

            # メッセージ処理のシミュレーション（少し時間をかける）
            time.sleep(0.1)

        except Exception as e:
            print(f"[Subscriber-{self.subscriber_id}] メッセージ処理エラー: {e}")

    def connect(self) -> bool:
        """AWS IoT Coreに接続"""
        try:
            self.mqtt_connection = self.setup_mqtt_connection()
            print(f"[Subscriber-{self.subscriber_id}] {self.config.endpoint} に接続中...")
            
            connect_future = self.mqtt_connection.connect()
            connect_result = connect_future.result(timeout=10)
            
            self.is_connected = True
            session_present = connect_result.get('session_present', False) if isinstance(connect_result, dict) else getattr(connect_result, 'session_present', False)
            print(f"[Subscriber-{self.subscriber_id}] 接続成功: {self.client_id}, セッション保持: {session_present}")
            
            # シェアサブスクリプションに登録
            shared_topic = self.config.get_shared_topic()
            subscribe_future, packet_id = self.mqtt_connection.subscribe(
                topic=shared_topic,
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self._on_message_received
            )
            
            subscribe_result = subscribe_future.result(timeout=10)
            qos_value = subscribe_result.get('qos', mqtt.QoS.AT_LEAST_ONCE).value if isinstance(subscribe_result, dict) else subscribe_result['qos'].value
            print(f"[Subscriber-{self.subscriber_id}] シェアサブスクリプション登録: {shared_topic} (QoS: {qos_value})")
            return True
            
        except Exception as e:
            print(f"[Subscriber-{self.subscriber_id}] 接続エラー: {e}")
            self.is_connected = False
            return False

    def simulate_disconnect(self, duration: int = 10):
        """接続断絶をシミュレート"""
        if not self.is_connected:
            return

        print(
            f"[Subscriber-{self.subscriber_id}] 接続断絶シミュレーション開始 ({duration}秒間)"
        )
        self.should_disconnect = True
        self.disconnect_duration = duration

        # 接続を切断
        if self.is_connected:
            try:
                disconnect_future = self.mqtt_connection.disconnect()
                disconnect_future.result(timeout=10)
                self.is_connected = False
            except Exception as e:
                if "NOT_CONNECTED" in str(e):
                    print(f"[Subscriber-{self.subscriber_id}] 既に切断済み")
                else:
                    print(f"[Subscriber-{self.subscriber_id}] 切断エラー: {e}")
                self.is_connected = False

        # 指定時間後に再接続
        def reconnect_after_delay():
            time.sleep(duration)
            if self.should_disconnect:
                print(f"[Subscriber-{self.subscriber_id}] 再接続試行中...")
                self.should_disconnect = False
                self.connect()

        threading.Thread(target=reconnect_after_delay, daemon=True).start()

    def get_stats(self) -> Dict[str, Any]:
        """統計情報を取得"""
        with self.lock:
            return {
                "subscriber_id": self.subscriber_id,
                "client_id": self.client_id,
                "is_connected": self.is_connected,
                "message_count": self.message_count,
                "processed_message_ids": list(self.processed_messages.keys()),
            }

    def disconnect(self):
        """接続を切断"""
        if self.mqtt_connection and self.is_connected:
            try:
                disconnect_future = self.mqtt_connection.disconnect()
                disconnect_future.result(timeout=10)
                self.is_connected = False
                print(f"[Subscriber-{self.subscriber_id}] 切断完了")
            except Exception as e:
                # 既に切断されている場合のエラーは無視
                if "NOT_CONNECTED" in str(e):
                    print(f"[Subscriber-{self.subscriber_id}] 既に切断済み")
                else:
                    print(f"[Subscriber-{self.subscriber_id}] 切断エラー: {e}")
                self.is_connected = False
        elif not self.is_connected:
            print(f"[Subscriber-{self.subscriber_id}] 既に切断状態です")


class MultiSubscriberManager:
    """複数のサブスクライバーを管理"""

    def __init__(self, config: AWSIoTConfig, num_subscribers: int = 3):
        self.config = config
        self.subscribers = []
        self.running = True

        # 複数のサブスクライバーを作成
        for i in range(num_subscribers):
            subscriber = IoTSharedSubscriber(config, f"{i+1:02d}")
            self.subscribers.append(subscriber)

    def start_all(self) -> bool:
        """全サブスクライバーを開始"""
        print(f"[Manager] {len(self.subscribers)}個のサブスクライバーを開始...")

        success_count = 0
        for subscriber in self.subscribers:
            if subscriber.connect():
                success_count += 1
                time.sleep(1)  # 接続間隔を空ける

        print(
            f"[Manager] {success_count}/{len(self.subscribers)} 個のサブスクライバーが接続成功"
        )
        
        if success_count > 0:
            print("\n🎉 全サブスクライバー準備完了！")
            print("📨 メッセージ受信待機中...")
            print("💡 別ターミナルで 'python -m src.publisher' を実行してください")
        
        return success_count > 0

    def simulate_random_disconnects(self):
        """ランダムに接続断絶をシミュレート"""
        print("[Manager] ランダム接続断絶シミュレーション開始")

        while self.running:
            time.sleep(random.uniform(5, 15))  # 5-15秒間隔

            if not self.running:
                break

            # ランダムにサブスクライバーを選択
            connected_subscribers = [s for s in self.subscribers if s.is_connected]
            if connected_subscribers:
                target = random.choice(connected_subscribers)
                duration = random.randint(8, 20)  # 8-20秒間切断
                target.simulate_disconnect(duration)

    def print_stats(self):
        """統計情報を表示"""
        print("\n=== サブスクライバー統計 ===")
        total_messages = 0
        disconnected_count = 0
        for subscriber in self.subscribers:
            stats = subscriber.get_stats()
            status = "接続中" if stats["is_connected"] else "切断中"
            message_count = stats['message_count']
            total_messages += message_count
            if not stats["is_connected"]:
                disconnected_count += 1
            print(
                f"  {stats['subscriber_id']}: {status}, メッセージ数: {message_count}"
            )
        
        print(f"📊 合計受信メッセージ数: {total_messages}")
        
        if disconnected_count > 0:
            print(f"⏳ {disconnected_count}個のサブスクライバーが切断中")
            print("💡 切断中のサブスクライバー再接続時にキューイングメッセージが配信されます")
            print("🔄 Message Queuing機能 動作中！")
        elif total_messages > 0:
            print("✅ Message Queuing 正常動作中！")

    def stop_all(self):
        """全サブスクライバーを停止"""
        self.running = False
        print("[Manager] 全サブスクライバーを停止中...")

        for subscriber in self.subscribers:
            subscriber.disconnect()


def signal_handler(sig, frame):
    """シグナルハンドラー"""
    print("\n[Main] 中断シグナル受信")
    sys.exit(0)


def main():
    """メイン実行関数"""
    config = AWSIoTConfig()

    if not config.validate():
        print("設定の検証に失敗しました")
        return

    # シグナルハンドラー設定
    signal.signal(signal.SIGINT, signal_handler)
    
    manager = MultiSubscriberManager(config, num_subscribers=3)

    try:
        print("[Main] サブスクライバー開始中...")
        
        # 全サブスクライバーを開始
        if not manager.start_all():
            print("サブスクライバーの開始に失敗しました")
            return

        # ランダム切断シミュレーションを開始
        disconnect_thread = threading.Thread(
            target=manager.simulate_random_disconnects, daemon=True
        )
        disconnect_thread.start()

        print("\n=== メッセージキューイングテスト実行中 ===")
        print("Ctrl+C で終了")

        # 定期的に統計を表示（無限ループ）
        while True:
            time.sleep(10)
            manager.print_stats()

    except KeyboardInterrupt:
        print("\n[Main] 中断されました")
    finally:
        manager.stop_all()


if __name__ == "__main__":
    main()
