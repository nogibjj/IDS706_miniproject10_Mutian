# PySpark Output

```
root
 |-- timestamp: long (nullable = true)
 |-- buy_vol: double (nullable = true)
 |-- sell_vol: double (nullable = true)
 |-- buy_sell_ratio: double (nullable = true)

+-------------+-------+--------+--------------+
|    timestamp|buy_vol|sell_vol|buy_sell_ratio|
+-------------+-------+--------+--------------+
|1690233300000|172.917| 125.523|        1.3776|
|1690233600000|483.703| 271.094|        1.7843|
|1690233900000|324.171| 223.937|        1.4476|
|1690234200000|115.396|  163.17|        0.7072|
|1690234500000|841.502| 499.905|        1.6833|
|1690234800000|198.966|  314.23|        0.6332|
|1690235100000| 41.116|   135.7|         0.303|
|1690235400000|300.176| 158.323|         1.896|
|1690235700000| 74.507|  51.221|        1.4546|
|1690236000000|445.995| 300.316|        1.4851|
|1690236300000| 102.42| 140.016|        0.7315|
|1690236600000|160.059| 203.265|        0.7874|
|1690236900000|108.816| 235.207|        0.4626|
|1690237200000|177.798| 320.623|        0.5545|
|1690237500000|249.369| 164.244|        1.5183|
|1690237800000| 137.54| 176.828|        0.7778|
|1690238100000| 51.585|  89.676|        0.5752|
|1690238400000| 48.225| 130.083|        0.3707|
|1690238700000| 62.638| 175.227|        0.3575|
|1690239000000| 133.43| 173.401|        0.7695|
+-------------+-------+--------+--------------+
only showing top 20 rows

+-----------+------------------+
|buyoversell|avg_buy_sell_ratio|
+-----------+------------------+
|          1|1.8283499026217247|
|          0|0.6428666995629502|
+-----------+------------------+

+-------------+-------+--------+--------------+
|    timestamp|buy_vol|sell_vol|buy_sell_ratio|
+-------------+-------+--------+--------------+
|1690239900000|171.781|  73.717|        2.3303|
|1690240800000|851.929| 302.976|        2.8119|
|1690244400000|288.746| 143.325|        2.0146|
|1690249500000|294.466|  66.575|        4.4231|
|1690251300000|496.143| 137.022|        3.6209|
|1690251600000|274.846| 133.317|        2.0616|
|1690251900000|357.166| 118.458|        3.0151|
|1690252200000|797.225| 293.349|        2.7177|
|1690257000000|302.651| 120.772|         2.506|
|1690260900000|110.988|  41.105|        2.7001|
|1690261800000|138.751|  35.611|        3.8963|
|1690262400000|152.832|  32.081|        4.7639|
|1690262700000|193.679|  84.728|        2.2859|
|1690263000000|274.244| 129.516|        2.1175|
|1690263900000|306.717| 141.338|        2.1701|
|1690266000000|217.437|   101.7|         2.138|
|1690273500000|230.347|  86.606|        2.6597|
|1690275000000|827.259| 366.015|        2.2602|
|1690276800000|715.785| 239.148|        2.9931|
|1690279500000|820.637| 374.831|        2.1894|
+-------------+-------+--------+--------------+
only showing top 20 rows


```
