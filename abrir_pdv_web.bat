@echo off
cd /d C:\Users\Pedro\OneDrive\Desktop\pdv_python
start http://localhost:8501
py -m streamlit run web_pdv.py --server.headless true
