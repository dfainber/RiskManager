# Metodologia ANO_EQ e DV01 — IDKA

Referência formal da métrica canônica de Renda Fixa da gestora.

## ANO_EQ (Ano Equivalente)

### Definição

Exposição a inflação/juros normalizada por AUM, em unidade de "anos". Interpretação direta:

> Se o ANO_EQ de um fundo é 3.0, uma variação de +1 bp nas taxas de juros gera perda de ~3 bps de cota.

### Fórmula

```
ANO_EQ = (AMOUNT × DV01 × 10000) / AUM
```

Onde:
- `AMOUNT`: quantidade física do instrumento (número de títulos, contratos futuros, etc.)
- `DV01`: valor presente do choque de 1 basis point no instrumento (em R$)
- `10000`: conversão de bp para unidade
- `AUM`: patrimônio líquido do fundo (em R$)

### Por que "ANO" equivalente

A métrica foi construída para fundos IPCA. A convenção é que:
- Um NTN-B com 3 anos de prazo tem aproximadamente 3 anos de "exposição IPCA"
- Um DI1 com 5 anos de prazo tem aproximadamente 5 anos de "exposição nominal"

Como o DV01 é proporcional ao prazo (aproximadamente), e a fórmula normaliza por AUM, o número final fica em uma escala interpretável: valores de 2-5 são comuns, e cada unidade pode ser lida como "um ano de exposição adicional".

### Comparação com métricas padrão

| Métrica | Unidade | Nível padrão para IDKA 3Y |
|---------|---------|---------------------------|
| DV01 do fundo | R$/bp | milhares de R$ |
| Duration modificada | anos | ~2.8 anos |
| **ANO_EQ** | anos | ~3.0 anos |

ANO_EQ é próximo de Duration Modificada, mas:
- Calculado bottom-up (soma dos DV01 × AMOUNT, normalizado por AUM)
- Trata DAP/NTN-B com ajuste de prorata IPCA
- Convenção interna da gestora

## DV01 por Instrumento

### NTN-B (Notas do Tesouro Nacional — Série B)

**Instrumento:** títulos públicos indexados ao IPCA, com juros periódicos.

**Cálculo:** usa a biblioteca `GLPG_Public_Bonds.NTNB`:

```python
from GLPG_Public_Bonds import NTNB, ServicosFinanceiros

servicos = ServicosFinanceiros(db_config={
    'hostname': 'GLPG-DB01', 'port': 5432,
    'database': 'DATA_DEV_DB',
    'username': 'svc_automation', 'password': 'admin',
})

ntnb = NTNB(expiration_date, dia_atual)
dv01 = ntnb.calcular_dv01(price, servicos)
```

A função interna considera:
- Pagamentos de cupom semestral
- Ajuste pro-rata do IPCA
- Curva de juros reais para desconto

### LFT (Letras Financeiras do Tesouro)

**Instrumento:** títulos indexados à SELIC, pós-fixados.

```python
from GLPG_Public_Bonds import LFT

lft = LFT(expiration_date, dia_atual)
dv01 = lft.calcular_dv01(price, servicos)
```

DV01 de LFT é tipicamente **baixo** — é um instrumento de liquidez, não de duration.

### DI1 (Futuros de DI)

**Instrumento:** futuros de juros nominais, negociados na B3.

**Cálculo manual** (não usa biblioteca):

```python
import numpy as np

# Para cada contrato:
price = contrato['PRICE']      # taxa em % (ex.: 11.50)
bdays = contrato['BDAYS']       # dias úteis até vencimento

# PU atual
base_value = 100000 / ((1 + price/100) ** (bdays/252))

# PU com choque de -1 bp
price_minus_bp = (price - 0.01) / 100
base_value_minus_bp = 100000 / ((1 + price_minus_bp) ** (bdays/252))

# DV01 = diferença
dv01 = np.abs(base_value - base_value_minus_bp)
```

Vetorizado em pandas:

```python
# Cálculo vetorizado para múltiplos contratos DI1
price_arr = df_di1['PRICE'].values
bdays_arr = df_di1['BDAYS'].values
base_value = 100000 / ((1 + price_arr/100) ** (bdays_arr/252))
price_minus_bp = (price_arr - 0.01) / 100
base_value_minus_bp = 100000 / ((1 + price_minus_bp) ** (bdays_arr/252))
df_di1['DV01'] = np.abs(base_value - base_value_minus_bp)
```

### DAP (Futuros de DI indexados ao IPCA — "NTN-B sintética")

**Instrumento:** futuros que funcionam como NTN-B sintética via choque em taxa real.

**Cálculo manual** com ajuste de prorata:

```python
prorata = get_prorata_ipca(dia_atual)  # vem de ECO_INDEX

# Mesmo cálculo do DI1 para base_value_minus_bp, mas com fator prorata
base_value_dv01 = 100000 / ((1 + price_minus_bp) ** (bdays/252))
df_dap['DV01'] = np.abs(base_value_dv01 * prorata * 0.00025)
```

O fator `0.00025` é a sensibilidade calibrada (equivalente a choque de juros reais de 1 bp convertido para PU).

### Posições em outros fundos (ex.: Albatroz)

**Pendente — a confirmar.**

Se o IDKA aloca em outro fundo (ex.: Albatroz), a posição aparece como um único PRODUCT. O DV01 dessa posição deveria vir:

- **Opção A:** lookup do DV01 do fundo alocado (look-through puro) — ideal mas requer acesso às posições internas do Albatroz
- **Opção B:** DV01 calculado como se fosse uma unidade de cota com duration média (proxy)
- **Opção C:** zero DV01 — tratar como caixa (errado, mas é o que acontece se não houver tratamento especial)

**Ação:** verificar na primeira execução como o script original trata. Se não trata, marcar como lacuna.

## Agregações

### Por book

```python
df['ANO_EQ'] = df['AMOUNT'] * df['DV01'] * 10000 / df['AUM']

ano_eq_por_book = df.groupby(['TRADING_DESK', 'BOOK'])['ANO_EQ'].sum()
```

### Por vértice (BDAYS)

```python
profile = df[df['BOOK'] == 'Benchmark_IDKA'].groupby('BDAYS')['ANO_EQ'].sum()
```

Para gráfico, usar `.cumsum()` para a curva acumulada.

## Validações

- **ANO_EQ total do book `Benchmark_IDKA` para IDKA 3Y** deveria ser ~3.0. Desvio significativo = problema.
- **ANO_EQ total do book `Benchmark_IDKA` para IDKA 10Y** deveria ser ~10.0 (ou o que a Anbima define para IDKA 10A).
- **ANO_EQ do book `ALL`** = ANO_EQ(Benchmark_IDKA) + ANO_EQ(RF_LF) (aproximadamente; pode haver diferenças pequenas por outras posições).

Se essas validações falharem:
- Algum instrumento pode estar sem DV01 calculado
- Pode haver posições em books não esperados
- Confirmar com gestão

## Extensão para outros fundos RF

A metodologia acima é **aplicável a todos os fundos RF da gestora** (Albatroz incluso). A diferença é:

- **IDKA:** ANO_EQ se aproxima do prazo do índice (3 ou 10)
- **Albatroz:** ANO_EQ pode variar amplamente (é um fundo ativo)
- **Outros RF:** depende do mandato

A skill futura `rf-albatroz-monitor` vai reutilizar essa mesma metodologia, só mudando o TRADING_DESK.
