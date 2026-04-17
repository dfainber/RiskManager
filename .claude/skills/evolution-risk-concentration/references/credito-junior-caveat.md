# Ressalva: VaR de Cotas Júnior de Crédito no Evolution

## O problema

O fundo Galapagos Evolution aloca parte do PL em **estratégia de Crédito Estruturado**, que pode incluir cotas júnior (tranches subordinadas) de FIDCs, securitizações e instrumentos similares.

**O sistema Lote45 captura o VaR desses instrumentos de forma que, historicamente, já distorceu o VaR agregado do Evolution em alguns momentos.** A distorção pode ser nos dois sentidos:

- **Subestimação:** cota júnior com marcação estável dá volatilidade histórica artificialmente baixa → VaR subestimado → diversification benefit aparente maior do que o real. O fundo parece mais diversificado do que é.
- **Superestimação:** eventos pontuais de marcação (ex.: downgrade, estresse no subjacente) geram spikes no histórico que podem inflar o VaR em janela 252d por meses. O fundo parece mais concentrado/arriscado do que está.

**Em qualquer dos dois casos, as três camadas desta skill ficam comprometidas:**

- **Camada 1** (percentil histórico do VaR): se a série de VaR da estratégia CREDITO tem ruído artificial, o percentil é interpretável
- **Camada 2** (diversification benefit): se o VaR do CREDITO está distorcido, a soma linear e o ratio ficam viesados
- **Camada 3** (correlação): se o PnL das cotas júnior não reflete o risco real (marcação "truncada"), a correlação com outras estratégias é falsa

## Status operacional

**Pode ou não ser problema hoje.** Não assumir que foi resolvido. A disciplina é:

1. Toda execução da skill verifica presença de cotas júnior na carteira
2. Se há, reporta no topo independentemente de magnitude
3. Se materialidade ≥ 5% do PL, calcula métricas complementares excluindo CREDITO
4. Nunca emite alerta forte sem considerar a ressalva

## Como identificar cotas júnior no banco

**Critério exato precisa ser calibrado.** Candidatos para filtro em `LOTE_BOOK_OVERVIEW`:

- `PRODUCT_CLASS` contendo termos como "FIDC", "CRI", "CRA", "Júnior", "Subordinada"
- `PRODUCT` com nome específico de tranche subordinada
- Cruzamento com tabela de cadastro de produtos (se existir coluna de subordinação)

**Ação na primeira execução da skill:**
1. Listar TODOS os `PRODUCT`/`PRODUCT_CLASS` do book `CREDITO`
2. Mostrar ao usuário para classificação manual
3. Registrar em `assets/cotas-junior-patterns.json` o filtro correto
4. Nas execuções seguintes, usar o filtro registrado

## Procedimento em cada execução

```python
def check_junior_credit_caveat(dia_atual, aum_evo):
    """
    Retorna dict com:
      - has_junior: bool
      - exposure_pct_pl: float (0-100)
      - var_contribuido_bps: float
      - flag_material: bool (True se exposure_pct_pl >= 5)
      - products_identified: list
      - recomendacao: str
    """
    # 1. Query de posições em CREDITO
    # 2. Filtrar pelo padrão em assets/cotas-junior-patterns.json
    # 3. Calcular exposição e VaR contribuído
    # 4. Gerar recomendação textual
    ...
```

Exemplo de saída quando há exposição material:

```
⚠️ CAVEAT: Cotas júnior de crédito na carteira

Exposição: 7.3% do PL
VaR contribuído: 8.5 bps (16% do VaR total do fundo)
Produtos identificados:
  - FIDC XPTO Junior: 4.1% PL
  - CRI Subordinada ABC: 3.2% PL

O VaR agregado do Evolution pode estar distorcido por limitação de
marcação a mercado dessas cotas. Ver análise ex-CREDITO nas camadas 1 e 2.
Confirmar com a equipe de risco antes de tirar conclusões finas.
```

## Cálculos complementares (ex-CREDITO)

Quando a flag material é ativada, calcular **três métricas adicionais**:

### 1. Camada 1 ex-CREDITO

```python
VaR_fundo_ex_credito = VaR_fundo - VaR_book_CREDITO
serie_historica_ex_credito = serie_VaR_fundo - serie_VaR_CREDITO
percentil_ex_credito = percentil(VaR_fundo_ex_credito_hoje, serie_historica_ex_credito)
```

Reportar lado a lado com o percentil "oficial". Se divergirem muito (ex.: P88 oficial vs. P45 ex-credito), o sinal está vindo do crédito e **pode não ser real**.

### 2. Camada 2 ex-CREDITO

```python
VaR_soma_ex_CRED = soma dos VaR das estratégias EXCETO CREDITO
VaR_real_ex_CRED = VaR_fundo - VaR_book_CREDITO  # aproximação linear; imperfeita
Ratio_ex_CRED = VaR_real_ex_CRED / VaR_soma_ex_CRED
```

**Limitação:** o `VaR_real_ex_CRED` aproximado por subtração é uma simplificação; o correto seria rodar o motor de VaR sem as posições de crédito, o que não é trivial. Usar a subtração como proxy e documentar no relatório.

### 3. Camada 3 ex-CREDITO

Na matriz de correlação, **adicionar asterisco** nas linhas/colunas do CREDITO. Não remover, para manter a visão completa — apenas sinalizar que os valores ali podem ser artefato.

## Governança

- **Atualização do critério de identificação:** quando a equipe de crédito introduz um novo instrumento, revisar `assets/cotas-junior-patterns.json`
- **Fechamento da ressalva:** se a equipe de risco estabelecer metodologia definitiva para VaR de cotas júnior (ex.: look-through para o subjacente, ou marcação baseada em CDS sintético), documentar aqui e mudar o status para "resolvido em [data]"
- **Histórico de casos:** toda vez que um relatório dessa skill foi ajustado por causa desse caveat, registrar em `change_log` dentro de `cotas-junior-patterns.json` — serve de memória institucional

## Quando essa ressalva for removida

Se a metodologia melhorar e o problema deixar de existir:

1. Atualizar este arquivo para status "Resolvido em YYYY-MM-DD"
2. Remover blocos condicionais do SKILL.md e queries auxiliares
3. Manter o arquivo histórico (não deletar) — serve de documentação para o "por que essa skill tem essa complexidade"

## Pontos em aberto (para discussão futura)

- [ ] Critério exato de identificação de cotas júnior no banco
- [ ] Qual o threshold de materialidade adequado (5%? 3%?)
- [ ] Se vale a pena calcular VaR ex-CREDITO "do zero" em vez de por subtração
- [ ] Relação com as outras estratégias de crédito que existem em outros fundos (Sea Lion, Dragon, Nazca, Iguana — se têm problema similar)
