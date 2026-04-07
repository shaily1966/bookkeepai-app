@echo off
echo Installing requirements...
pip install -r requirements.txt

echo Starting ContractCheckPro...
streamlit run app.py

pause