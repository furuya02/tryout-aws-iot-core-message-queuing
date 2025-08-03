import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class AWSIoTConfig:
    """AWS IoT Core設定クラス"""
    
    def __init__(self):
        # srcディレクトリからの相対パスでcertsディレクトリを参照
        current_dir = os.path.dirname(os.path.abspath(__file__))
        certs_dir = os.path.join(current_dir, 'certs')
        
        self.endpoint: str = os.getenv('AWS_IOT_ENDPOINT', 'your-iot-endpoint.iot.region.amazonaws.com')
        self.root_ca_path: str = os.getenv('ROOT_CA_PATH', os.path.join(certs_dir, 'AmazonRootCA1.pem'))
        self.cert_path: str = os.getenv('CERT_PATH', os.path.join(certs_dir, 'device.pem.crt'))
        self.private_key_path: str = os.getenv('PRIVATE_KEY_PATH', os.path.join(certs_dir, 'private.pem.key'))
        self.client_id_prefix: str = os.getenv('CLIENT_ID_PREFIX', 'message-queuing-test')
        self.topic_prefix: str = os.getenv('TOPIC_PREFIX', 'test/shared')
        self.shared_subscription_group: str = os.getenv('SHARED_GROUP', 'message-queuing-group')
        
    def get_shared_topic(self) -> str:
        """シェアサブスクリプション用のトピック名を取得"""
        return f"$share/{self.shared_subscription_group}/{self.topic_prefix}/messages"
    
    def get_publish_topic(self) -> str:
        """パブリッシュ用のトピック名を取得"""
        return f"{self.topic_prefix}/messages"
    
    def validate(self) -> bool:
        """設定の妥当性を確認"""
        required_files = [self.root_ca_path, self.cert_path, self.private_key_path]
        for file_path in required_files:
            if not os.path.exists(file_path):
                print(f"証明書ファイルが見つかりません: {file_path}")
                return False
        return True