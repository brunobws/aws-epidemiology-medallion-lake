@echo off
title EpiMind Dashboard
echo ===================================================
echo Iniciando o ambiente virtual e o EpiMind Dashboard...
echo ===================================================

:: Verifica se o ambiente virtual existe. Se não existir, cria e instala as dependências.
if not exist ".venv\Scripts\activate.bat" (
    echo [!] Ambiente virtual nao encontrado. Criando um novo em .\.venv...
    python -m venv .venv
    echo [!] Instalando dependencias...
    call .\.venv\Scripts\activate.bat
    pip install -r requirements.txt
) else (
    call .\.venv\Scripts\activate.bat
)

:: Roda o aplicativo Streamlit
streamlit run streamlit_app/main.py

pause
