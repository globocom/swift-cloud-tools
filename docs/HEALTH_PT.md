## Serviço Health (Balanceamento de DNS via AWS Route 53)

### Critérios para mudança nos pesos

Teremos sempre 3 níveis configurados: Baixo, Médio e Alto.

(Valores de exemplo)

|                    | Baixo           | Médio                | Alto                  |
| ------------------ | --------------- | -------------------- | --------------------- |
| uso de cpu         | entre 0% e 20%  | entre 21% e 40%      | entre 41% e 100%      |
| número de conexões | entre 0 e 90000 | entre 90001 e 180000 | entre 180001 e 999999 |
| peso Globo         | 255             | 235                  | 200                   |
| peso CGP           | 1               | 20                   | 55                    |

### Passo a passo

-   O serviço é executado num intervalo fixo (padrão 10 min);
-   cada vez que é executado, busca os pesos atuais para Globo e GCP;
-   em seguida é verificado se estes pesos estão nos níveis Baixo, Médio ou Alto;
-   verificamos então o uso de cpu e o número de conexões em cada host FE;
-   comparamos estes números com os níveis citados acima;
-   caso o nível verificado esteja diferente do atual, alteramos o peso sempre utilizando o nivel Médio como passo seguinte. O peso nunca é alterando diretamente de Baixo para Alto e vice-versa.
