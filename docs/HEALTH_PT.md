## Serviço Health (Balanceamento de DNS via AWS Route 53)

### Critérios para mudança nos pesos

Teremos sempre 3 níveis configurados: Baixo, Médio e Alto.

(Valores de exemplo)

|                    | Baixo             | Médio              | Alto                |
| ------------------ | ----------------- | ------------------ | ------------------- |
| load               | entre 0.00 e 1.00 | entre 1.01 e 2.00  | entre 2.01 e 99.99  |
| número de conexões | entre 0 e 5000    | entre 5001 e 15000 | entre 15001 e 99999 |
| peso Globo         | 255               | 225                | 200                 |
| peso CGP           | 0                 | 30                 | 55                  |

### Passo a passo

-   O serviço é executado num intervalo fixo (padrão 10 min);
-   cada vez que é executado, busca os pesos atuais para Globo e GCP;
-   em seguida é verificado se estes pesos estão nos níveis Baixo, Médio ou Alto;
-   verificamos então o número de conexões e o load em cada host FE;
-   comparamos estes números com os níveis citados acima;
-   caso o nível verificado esteja diferente do atual, alteramos o peso sempre utilizando o nivel Médio como passo seguinte. O peso nunca é alterando diretamente de Baixo para Alto e vice-versa.
