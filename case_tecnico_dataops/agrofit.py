
import pandas as pd

# 1. CARREGAR OS DADOS !!

def carregar_dados(caminho):
    try:
        df = pd.read_csv(caminho, sep=';', encoding='utf-8')
        print("✅ Dados carregados com sucesso")
        return df
    except Exception as e:
        print(f"❌ Erro ao carregar dados: {e}")
        return None


# 2. TRATAR DADOS ...

def tratar_dados(df):
    
    # padronizar nomes das colunas
    df.columns = df.columns.str.lower().str.strip()
    
    # remover duplicados
    df = df.drop_duplicates()
    
    # padronizar textos
    colunas_texto = ['titular_de_registro', 'cultura']
    
    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
    
    # tratar nulos (apenas texto)
    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].fillna('NAO INFORMADO')
    
    return df


# 3. VALIDAR DADOS ...

def validar_dados(df):
    print("\n📊 INFORMAÇÕES GERAIS")
    print(f"Total de linhas: {len(df)}")
    
    print("\n🔍 VALORES NULOS POR COLUNA:")
    print(df.isnull().sum())

    # validação de colunas essenciais
    colunas_necessarias = ['titular_de_registro', 'cultura']
    
    for col in colunas_necessarias:
        if col not in df.columns:
            print(f"⚠️ Aviso: coluna '{col}' não encontrada na base")


# 4. ANÁLISE ...

def analisar_dados(df):
    
    print("\n🏢 TOP 10 EMPRESAS:")
    if 'titular_de_registro' in df.columns:
        print(df['titular_de_registro'].value_counts().head(10))
    
    print("\n🌱 TOP 10 CULTURAS:")
    if 'cultura' in df.columns:
        print(df['cultura'].value_counts().head(10))
    
    print("\n⚠️ CLASSE AMBIENTAL:")
    if 'classe_ambiental' in df.columns:
        print(df['classe_ambiental'].value_counts())


# 5. MÉTRICAS...

def calcular_risco(df):
    
    if 'classe_ambiental' in df.columns:
        risco = df['classe_ambiental'].value_counts(normalize=True) * 100
        
        print("\n📊 % DISTRIBUIÇÃO CLASSE AMBIENTAL:")
        print(risco.round(2))
    else:
        print("⚠️ Coluna 'classe_ambiental' não encontrada para cálculo de risco")


# 6. SALVAR DADOS ...

def salvar_dados(df, caminho_saida):
    df.to_csv(caminho_saida, index=False)
    print(f"\n💾 Arquivo salvo em: {caminho_saida}")


# 7. FUNÇÃO PRINCIPAL ...

def main():
    
    print("\n🚀 Iniciando pipeline...\n")
    
    caminho_entrada = 'agrofitprodutosformulados.csv'
    caminho_saida = 'agrofit_tratado.csv'
    
    # carregar
    df = carregar_dados(caminho_entrada)
    
    if df is None:
        return
    
    # validar antes
    validar_dados(df)
    
    # tratar
    df = tratar_dados(df)
    
    # análise
    analisar_dados(df)
    
    # métricas
    calcular_risco(df)
    
    # salvar base completa
    salvar_dados(df, caminho_saida)
    
    ### GERAR AMOSTRA PARA GITHUB
    df.sample(min(1000, len(df))).to_csv("sample_agrofit.csv", index=False)
    print("📦 Amostra salva: sample_agrofit.csv")


# EXECUTAR ...

if __name__ == "__main__":
    main()
    