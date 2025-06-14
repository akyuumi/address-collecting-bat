#!/bin/bash

# スクリプトの絶対パスを取得
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 仮想環境のパスを設定
VENV_PATH="$SCRIPT_DIR/.venv"

# 仮想環境が存在しない場合は作成
if [ ! -d "$VENV_PATH" ]; then
    python3 -m venv "$VENV_PATH"
fi

# 仮想環境をアクティベート
source "$VENV_PATH/bin/activate"

# パッケージをインストール
pip install -r requirements.txt

# メインスクリプトを実行
python main.py

# cronの設定
CRON_JOB="30 18 * * * $SCRIPT_DIR/execute.bash >> $SCRIPT_DIR/cron.log 2>&1"

# 既存のcronジョブを確認
EXISTING_CRON=$(crontab -l 2>/dev/null)

# 既に同じジョブが設定されていないか確認
if ! echo "$EXISTING_CRON" | grep -q "$SCRIPT_DIR/execute.bash"; then
    # 新しいcronジョブを追加
    (echo "$EXISTING_CRON"; echo "$CRON_JOB") | crontab -
    echo "Cron job has been set to run daily at 18:30"
else
    echo "Cron job already exists"
fi



