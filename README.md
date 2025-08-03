
# AWS IoT Core Message Queuing Test Suite

AWS IoT Core の新機能「Message Queuing for MQTT Shared Subscriptions」を検証・テストするための包括的なプログラム群です。

## 🎯 プロジェクト概要

このテストスイートは、2024年7月31日にリリースされたAWS IoT Core Message Queuing機能の動作を実証・検証します：

- **シェアサブスクリプション**: 複数のサブスクライバー間でのメッセージ配信負荷分散
- **メッセージキューイング**: サブスクライバー切断時のメッセージ保持機能
- **永続セッション**: Clean Session=false による接続状態の維持
- **QoS1保証**: メッセージ配信保証とキューイング機能の連携

## 🏗️ アーキテクチャ

### コアコンポーネント

- **`src/config.py`**: AWS IoT設定管理と証明書パス解決
- **`src/publisher.py`**: AWS IoT Device SDK v2を使用したQoS1メッセージ送信（共有サブスクリプション対応）
- **`src/subscriber.py`**: AWS IoT Device SDK v2を使用した複数共有サブスクライバーと切断シミュレーション


### メッセージフロー

```
Publisher → test/shared/messages
    ↓
AWS IoT Core (Message Queuing)
    ↓
$share/message-queuing-group/test/shared/messages → Multiple Subscribers
```

## 🚀 セットアップ

### 前提条件

1. **Python 3.7+**（AWS IoT Device SDK v2 要件）
2. **AWS CLI**がインストール・設定済み
3. **適切なIAMポリシー**（AWS IoT操作権限が必要）

**必要なIAM権限**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iot:CreateThing",
        "iot:DescribeThing",
        "iot:CreateKeysAndCertificate",
        "iot:CreatePolicy",
        "iot:AttachPolicy",
        "iot:AttachThingPrincipal",
        "iot:DescribeEndpoint"
      ],
      "Resource": "*"
    }
  ]
}
```

## 📋 AWS IoT Thing セットアップ

### 1. AWS IoT Thing作成

```bash
# Thing作成
aws iot create-thing --thing-name message-queuing-test-device

# Thing ARNを確認（後で使用）
aws iot describe-thing --thing-name message-queuing-test-device
```

### 2. 証明書とキーの作成

```bash
# certsディレクトリを作成
mkdir -p src/certs
cd src/certs

# 証明書作成とダウンロード
aws iot create-keys-and-certificate --set-as-active \
  --certificate-pem-outfile device.pem.crt \
  --private-key-outfile private.pem.key \
  --public-key-outfile public.pem.key

# Amazon Root CA証明書をダウンロード
curl -o AmazonRootCA1.pem https://www.amazontrust.com/repository/AmazonRootCA1.pem

# 証明書ARNを記録（次のステップで使用）
# コマンド実行時の出力から certificateArn をコピーしてください
```

### 3. IoTポリシーの作成とアタッチ

```bash
# IoTポリシーを作成（プロジェクトルートのpolicy.jsonを使用）
aws iot create-policy --policy-name MessageQueuingTestPolicy --policy-document file://policy.json

# 証明書にポリシーをアタッチ（<CERTIFICATE_ARN>を実際の値に置き換え）
aws iot attach-policy --policy-name MessageQueuingTestPolicy --target <CERTIFICATE_ARN>

# 証明書をThingにアタッチ（<CERTIFICATE_ARN>を実際の値に置き換え）
aws iot attach-thing-principal --thing-name message-queuing-test-device --principal <CERTIFICATE_ARN>
```

**既存のポリシーを更新する場合:**
```bash
# ポリシーを更新（既に作成済みの場合）
aws iot create-policy-version --policy-name MessageQueuingTestPolicy --policy-document file://policy.json --set-as-default
```

**policy.json** ファイルの内容:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iot:Connect"
      ],
      "Resource": "arn:aws:iot:*:*:client/message-queuing-test-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iot:Publish"
      ],
      "Resource": [
        "arn:aws:iot:*:*:topic/test/shared/messages"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "iot:Subscribe"
      ],
      "Resource": [
        "arn:aws:iot:*:*:topicfilter/$share/message-queuing-group/test/shared/messages"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "iot:Receive"
      ],
      "Resource": [
        "arn:aws:iot:*:*:topic/$share/message-queuing-group/test/shared/messages"
      ]
    }
  ]
}
```

### 4. AWS IoTエンドポイントの取得

```bash
# IoTエンドポイントを取得
aws iot describe-endpoint --endpoint-type iot:Data-ATS

# 出力例: xxxxx-ats.iot.ap-northeast-1.amazonaws.com
```

### 5. 環境設定ファイルの作成

```bash
# プロジェクトルートに戻る
cd ../..

# .envファイルを作成
cp src/.env.example src/.env

# .envファイルを編集して実際のエンドポイントを設定
# AWS_IOT_ENDPOINT=your-actual-endpoint.iot.ap-northeast-1.amazonaws.com
```

## 🛠️ 環境構築

```bash
# 仮想環境作成・有効化
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 依存関係インストール
pip3 install -r src/requirements.txt

# 環境設定
cp src/.env.example src/.env
# .envファイルを編集してAWS IoTエンドポイントを設定
```

## 📋 使用方法

### Message Queuing 動作確認手順

AWS IoT Core の Message Queuing 機能を手動で確認する手順です。

#### ステップ1: サブスクライバーを起動

```bash
# ターミナル1でサブスクライバーを起動
python -m src.subscriber
```

サブスクライバーが起動すると、以下のような出力が表示されます：
```
[Main] サブスクライバー開始中...
[Manager] 3個のサブスクライバーが接続成功
[Subscriber-01] 接続成功: message-queuing-test-subscriber-01, セッション保持: True
[Subscriber-02] 接続成功: message-queuing-test-subscriber-02, セッション保持: True
[Subscriber-03] 接続成功: message-queuing-test-subscriber-03, セッション保持: True

=== メッセージキューイングテスト実行中 ===
Ctrl+C で終了
```

**サブスクライバーはCtrl+Cで停止するまで継続的に動作します。**

**⚠️ 重要**: サブスクライバーが「🎉 全サブスクライバー準備完了！」メッセージを表示するまで待ってから、パブリッシャーを起動してください。

#### ステップ2: パブリッシャーを起動（別ターミナル）

```bash
# ターミナル2でパブリッシャーを起動
python -m src.publisher
```

パブリッシャーが20個のメッセージを送信し、サブスクライバーでメッセージ受信が確認できます。

#### ステップ3: Message Queuing 確認

1. **サブスクライバーを意図的に停止**（Ctrl+C）
2. **パブリッシャーで再度メッセージ送信**
   ```bash
   python -m src.publisher
   ```
3. **サブスクライバーを再起動**
   ```bash
   python -m src.subscriber
   ```
4. **キューイングされたメッセージが配信されることを確認**


### 🔍 確認ポイント

- **シェアサブスクリプション**: 複数サブスクライバー間でのメッセージ負荷分散
- **永続セッション**: `セッション保持: True` の確認
- **メッセージキューイング**: 切断中のメッセージが再接続時に配信される
- **QoS1保証**: 確実なメッセージ配信

### 📊 **期待される結果**

Publisher が20件送信した場合の正常な動作：

```
=== サブスクライバー統計 ===
  01: 接続中, メッセージ数: 7
  02: 切断中, メッセージ数: 4    ← 切断中（Message Queuing中）
  03: 接続中, メッセージ数: 9
📊 合計受信メッセージ数: 20（または18-20件）
⏳ 1個のサブスクライバーが切断中
💡 切断中のサブスクライバー再接続時にキューイングメッセージが配信されます
🔄 Message Queuing機能 動作中！
```

**重要**: 
- 合計が20件未満の場合、切断中のサブスクライバーにメッセージがキューイングされています
- これは **Message Queuing機能が正常動作している証拠** です
- 切断されたサブスクライバーが再接続すると、不足分のメッセージが配信されます

### ✅ **検証完了**

このテストスイートは実際に動作確認済みです：
- ✅ パブリッシャー: 20件のメッセージを全て送信完了
- ✅ 永続セッション（セッション保持: True）による接続
- ✅ QoS1による確実な配信確認
- ✅ シェアサブスクリプション負荷分散動作
- ✅ Message Queuing機能による切断時メッセージ保持


## ⚙️ 設定

### 環境変数

| 変数名 | 説明 | デフォルト値 |
|--------|------|-------------|
| `AWS_IOT_ENDPOINT` | AWS IoTエンドポイント | （必須） |
| `CLIENT_ID_PREFIX` | MQTTクライアントIDプレフィックス | `message-queuing-test` |
| `TOPIC_PREFIX` | メッセージトピックプレフィックス | `test/shared` |
| `SHARED_GROUP` | シェアサブスクリプショングループ名 | `message-queuing-group` |
| `NUM_SUBSCRIBERS` | サブスクライバー数 | `3` |

### 重要な技術仕様

- **SDK**: AWS IoT Device SDK v2（paho-mqttから移行済み）
- **Clean Session**: `false`（永続セッション必須）
- **QoS**: `1`（メッセージキューイング要件）
- **TLS**: 必須（AWS IoT証明書認証）
- **シェアサブスクリプション形式**: `$share/{group}/{topic}`
- **非同期処理**: Future-based操作によるパフォーマンス向上

## 📁 ディレクトリ構造

```
├── src/
│   ├── certs/           # AWS IoT証明書ファイル
│   │   ├── AmazonRootCA1.pem
│   │   ├── device.pem.crt
│   │   └── private.pem.key
│   ├── config.py        # 設定管理
│   ├── publisher.py     # メッセージ送信（手動テスト用）
│   ├── subscriber.py    # メッセージ受信（手動テスト用）
│   ├── requirements.txt # Python依存関係
│   ├── .env.example     # 環境設定テンプレート
│   └── .env             # 実際の環境設定（ユーザー作成）
├── policy.json         # AWS IoTポリシー定義
├── CLAUDE.md           # Claude Code用ガイダンス
└── README.md           # プロジェクト説明（本ファイル）
```

## 🔍 主な検証ポイント

1. **メッセージキューイング動作**: サブスクライバー切断時のメッセージ保持
2. **シェアサブスクリプション**: 複数サブスクライバー間でのメッセージ配信負荷分散
3. **永続セッション**: 再接続時のキューメッセージ配信
4. **QoS1保証**: メッセージ配信確実性

## 🐛 トラブルシューティング

### よくある問題

- **接続エラー**: AWS IoTエンドポイントと証明書ファイルの確認
- **メッセージ未受信**: Clean Session設定とQoS設定の確認
- **キューイング未動作**: 永続セッション（Clean Session=false）の確認

### デバッグ方法

各コンポーネントは詳細なログを出力します。問題発生時はログを確認してください。

## 📄 ライセンス

このプロジェクトはMITライセンスの下で公開されています。