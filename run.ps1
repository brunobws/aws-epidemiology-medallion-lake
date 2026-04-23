Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "Iniciando o ambiente virtual e o EpiMind Dashboard..." -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

# Verifica se o ambiente virtual existe. Se não existir, cria e instala dependências
if (-Not (Test-Path ".\.venv\Scripts\Activate.ps1")) {
    Write-Host "[!] Ambiente virtual não encontrado. Criando um novo em .\.venv..." -ForegroundColor Yellow
    python -m venv .venv
    Write-Host "[!] Ativando e instalando dependências (isso pode demorar um pouco na primeira vez)..." -ForegroundColor Yellow
    .\.venv\Scripts\Activate.ps1
    pip install -r requirements.txt
} else {
    .\.venv\Scripts\Activate.ps1
}

# Roda o aplicativo Streamlit
streamlit run streamlit_app/main.py
