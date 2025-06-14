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


