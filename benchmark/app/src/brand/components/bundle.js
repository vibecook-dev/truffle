/* @ds-bundle: {"format":4,"namespace":"JY","components":[{"name":"Mark"},{"name":"Wordmark"},{"name":"Lockup"},{"name":"Loader"},{"name":"Blueprint"},{"name":"Measure"},{"name":"Button"},{"name":"TextLink"},{"name":"Tag"},{"name":"Kbd"},{"name":"Icon"},{"name":"ProjectCard"}]} */
/* James Yong identity, components. One classic script; reads window.React; assigns window.JY. */
(function () {
  'use strict';
  var React = window.React;
  var h = React.createElement;
  var useState = React.useState, useEffect = React.useEffect, useLayoutEffect = React.useLayoutEffect, useRef = React.useRef;

  /* Geometry of every logo form, in mark units (square = 1000), SVG y-down. Generated from the v1 mark. */
  var GEO = {"row":{"d":"M0 552a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0ZM1122 1062L2285 1062L1703.5 0ZM2463 1052L3463 1052L3463 52L2463 52Z","box":[0.0,0.0,3463.0,1083.0],"baseline":1052,"capline":52,"square":1000,"parts":[{"kind":"c","d":"M0 552a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0Z","box":[0.0,21.0,1062.0,1083.0]},{"kind":"t","d":"M1122 1062L2285 1062L1703.5 0Z","box":[1122.0,0.0,2285.0,1062.0]},{"kind":"s","d":"M2463 1052L3463 1052L3463 52L2463 52Z","box":[2463.0,52.0,3463.0,1052.0]}]},"wrap":{"d":"M0 552a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0ZM1122 1062L2285 1062L1703.5 0ZM642.5 2283L1642.5 2283L1642.5 1283L642.5 1283Z","box":[0.0,0.0,2285.0,2283.0],"baseline":2283.0,"baseline1":1052,"capline":52,"square":1000,"rowGap":200,"g1":60,"parts":[{"kind":"c","d":"M0 552a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0Z","box":[0.0,21.0,1062.0,1083.0]},{"kind":"t","d":"M1122 1062L2285 1062L1703.5 0Z","box":[1122.0,0.0,2285.0,1062.0]},{"kind":"s","d":"M642.5 2283L1642.5 2283L1642.5 1283L642.5 1283Z","box":[642.5,1283.0,1642.5,2283.0]}]},"wrapMicro":{"d":"M0 552a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0ZM1222 1062L2385 1062L1803.5 0ZM692.5 2323L1692.5 2323L1692.5 1323L692.5 1323Z","box":[0.0,0.0,2385.0,2323.0],"baseline":2323.0,"baseline1":1052,"capline":52,"square":1000,"rowGap":240,"g1":160,"parts":[{"kind":"c","d":"M0 552a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0Z","box":[0.0,21.0,1062.0,1083.0]},{"kind":"t","d":"M1222 1062L2385 1062L1803.5 0Z","box":[1222.0,0.0,2385.0,1062.0]},{"kind":"s","d":"M692.5 2323L1692.5 2323L1692.5 1323L692.5 1323Z","box":[692.5,1323.0,1692.5,2323.0]}]},"wordmark":{"d":"M600 55.36L600 776.79L599.64 791.51L598.56 806.19L596.75 820.8L594.24 835.31L591.01 849.68L587.08 863.87L582.46 877.85L577.16 891.59L571.2 905.05L564.58 918.2L557.32 931.02L549.44 943.46L540.96 955.5L531.9 967.1L522.29 978.25L512.13 988.92L501.47 999.07L490.32 1008.69L478.71 1017.75L466.67 1026.23L454.23 1034.1L441.42 1041.36L428.27 1047.98L414.81 1053.95L401.07 1059.25L387.09 1063.87L372.89 1067.8L358.53 1071.02L344.02 1073.54L329.41 1075.34L314.72 1076.42L300 1076.79L285.28 1076.42L270.59 1075.34L255.98 1073.54L241.47 1071.02L227.11 1067.8L212.91 1063.87L198.93 1059.25L185.19 1053.95L171.73 1047.98L158.58 1041.36L145.77 1034.1L133.33 1026.23L121.29 1017.75L109.68 1008.69L98.53 999.07L87.87 988.92L77.71 978.25L68.1 967.1L59.04 955.5L50.56 943.46L42.68 931.02L35.42 918.2L28.8 905.05L22.84 891.59L17.54 877.85L12.92 863.87L8.99 849.68L5.76 835.31L3.25 820.8L1.44 806.19L0.36 791.51L0 776.79L173.29 776.79L173.44 783.89L173.9 790.97L174.66 798.02L175.72 805.02L177.08 811.95L178.74 818.79L180.69 825.54L182.93 832.17L185.45 838.66L188.25 845L191.31 851.18L194.64 857.18L198.22 862.99L202.05 868.59L206.11 873.97L210.4 879.11L214.9 884.01L219.61 888.65L224.52 893.02L229.6 897.11L234.86 900.91L240.27 904.41L245.82 907.61L251.51 910.48L257.31 913.04L263.22 915.27L269.21 917.16L275.28 918.72L281.41 919.93L287.58 920.8L293.78 921.33L300 921.5L306.22 921.33L312.42 920.8L318.59 919.93L324.72 918.72L330.79 917.16L336.78 915.27L342.69 913.04L348.49 910.48L354.18 907.61L359.73 904.41L365.14 900.91L370.4 897.11L375.48 893.02L380.39 888.65L385.1 884.01L389.6 879.11L393.89 873.97L397.95 868.59L401.78 862.99L405.36 857.18L408.69 851.18L411.75 845L414.55 838.66L417.07 832.17L419.31 825.54L421.26 818.79L422.92 811.95L424.28 805.02L425.34 798.02L426.1 790.97L426.56 783.89L426.71 776.79L426.71 55.36ZM1010.07 798.21H1538.64L1506.5 656.07H1042.21ZM1271.5 338.21 1430.78 709.29 1434 751.07 1569.71 1055.36H1760.78L1271.5 0L782.21 1055.36H973.28L1111.86 742.14L1114.36 705ZM2134.87 446.43 2439.51 962.5 2744.15 446.43 2812.72 1055.36H2987.72L2850.22 5.36L2439.51 683.57L2028.8 5.36L1891.3 1055.36H2066.3ZM3327.35 1055.36H3831.63V907.14H3327.35ZM3327.35 203.57H3831.63V55.36H3327.35ZM3327.35 592.86H3803.06V446.43H3327.35ZM3222.71 55.36V1055.36H3388.78V55.36ZM4096.37 733.21 3970.65 821.07Q4003.87 889.29 4059.76 948.04Q4115.65 1006.79 4190.65 1041.79Q4265.65 1076.79 4353.51 1076.79Q4417.8 1076.79 4477.26 1056.43Q4536.72 1036.07 4583.87 997.86Q4631.01 959.64 4658.33 904.11Q4685.65 848.57 4685.65 778.57Q4685.65 712.5 4663.15 663.57Q4640.65 614.64 4603.87 578.75Q4567.08 542.86 4522.08 518.39Q4477.08 493.93 4432.08 477.86Q4352.08 450 4301.9 422.86Q4251.72 395.71 4228.33 364.64Q4204.94 333.57 4204.94 295Q4204.94 252.5 4237.97 220.18Q4271.01 187.86 4340.3 187.86Q4391.01 187.86 4428.69 207.5Q4466.37 227.14 4494.76 259.11Q4523.15 291.07 4542.8 327.86L4679.22 250.71Q4654.22 198.21 4609.05 149.46Q4563.87 100.71 4498.15 69.11Q4432.44 37.5 4343.15 37.5Q4253.87 37.5 4182.8 71.25Q4111.72 105 4070.65 165.71Q4029.58 226.43 4029.58 306.79Q4029.58 377.14 4056.37 426.43Q4083.15 475.71 4124.76 509.46Q4166.37 543.21 4212.26 564.82Q4258.15 586.43 4294.94 599.64Q4358.15 622.5 4406.19 645.89Q4454.22 669.29 4481.19 702.86Q4508.15 736.43 4508.15 791.79Q4508.15 851.43 4465.3 888.39Q4422.44 925.36 4353.15 925.36Q4297.08 925.36 4250.83 901.43Q4204.58 877.5 4166.9 834.11Q4129.22 790.71 4096.37 733.21ZM5897.17 55.36 5663.96 483.21 5431.82 55.36H5238.96L5578.96 635.36V1055.36H5750.39V633.57L6090.03 55.36ZM6398.69 555.36Q6398.69 452.14 6442.44 371.96Q6486.19 291.79 6562.98 245.54Q6639.76 199.29 6738.69 199.29Q6839.76 199.29 6915.84 245.54Q6991.91 291.79 7035.3 371.96Q7078.69 452.14 7078.69 555.36Q7078.69 658.21 7034.94 738.57Q6991.19 818.93 6914.94 865.18Q6838.69 911.43 6738.69 911.43Q6639.76 911.43 6562.98 865.18Q6486.19 818.93 6442.44 738.57Q6398.69 658.21 6398.69 555.36ZM6219.05 555.36Q6219.05 666.79 6257.62 762.32Q6296.19 857.86 6366.19 928.04Q6436.19 998.21 6531.37 1037.5Q6626.55 1076.79 6738.69 1076.79Q6851.91 1076.79 6946.55 1037.5Q7041.19 998.21 7111.19 928.04Q7181.19 857.86 7219.76 762.32Q7258.34 666.79 7258.34 555.36Q7258.34 442.86 7219.59 348.39Q7180.84 253.93 7110.48 184.46Q7040.12 115 6945.66 76.25Q6851.19 37.5 6738.69 37.5Q6627.98 37.5 6532.98 76.25Q6437.98 115 6367.62 184.46Q6297.26 253.93 6258.16 348.39Q6219.05 442.86 6219.05 555.36ZM8225.55 55.36V726.79L7495.2 5.36V1055.36H7661.27V383.93L8391.63 1105.36V55.36ZM9162.73 675H9468.8Q9455.95 725.71 9430.41 770Q9404.87 814.29 9366.48 848.21Q9328.09 882.14 9275.05 900.89Q9222.02 919.64 9155.59 919.64Q9057.02 919.64 8978.27 873.93Q8899.52 828.21 8853.45 746.07Q8807.37 663.93 8807.37 555.36Q8807.37 445.36 8854.16 364.82Q8900.95 284.29 8979.87 239.46Q9058.8 194.64 9155.59 194.64Q9259.16 194.64 9332.73 237.32Q9406.3 280 9448.09 351.79L9585.59 260Q9540.95 190 9477.73 140.54Q9414.52 91.07 9334.16 64.29Q9253.8 37.5 9155.59 37.5Q9042.73 37.5 8946.48 75Q8850.23 112.5 8778.98 181.61Q8707.73 250.71 8668.27 345.89Q8628.8 441.07 8628.8 555.36Q8628.8 669.64 8667.73 765.36Q8706.66 861.07 8777.2 930.89Q8847.73 1000.71 8943.45 1038.75Q9039.16 1076.79 9152.02 1076.79Q9271.66 1076.79 9364.7 1033.39Q9457.73 990 9520.95 914.29Q9584.16 838.57 9615.41 740.54Q9646.66 642.5 9643.8 532.14H9162.73Z","box":[0,0,9644.04,1105.36],"baseline":1055.357142857143,"capline":55.35714285714289,"cap":1000,"width":9644.042325250342,"tracking":"equal white 30% of cap height, depth 12%","font":"Jost Medium (500), baseline J"},"inline":{"d":"M0 555.36a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0ZM1122 1065.36L2285 1065.36L1703.5 3.36ZM2463 1055.36L3463 1055.36L3463 55.36L2463 55.36ZM5063 55.36L5063 776.79L5062.64 791.51L5061.56 806.19L5059.75 820.8L5057.24 835.31L5054.01 849.68L5050.08 863.87L5045.46 877.85L5040.16 891.59L5034.2 905.05L5027.58 918.2L5020.32 931.02L5012.44 943.46L5003.96 955.5L4994.9 967.1L4985.29 978.25L4975.13 988.92L4964.47 999.07L4953.32 1008.69L4941.71 1017.75L4929.67 1026.23L4917.23 1034.1L4904.42 1041.36L4891.27 1047.98L4877.81 1053.95L4864.07 1059.25L4850.09 1063.87L4835.89 1067.8L4821.53 1071.02L4807.02 1073.54L4792.41 1075.34L4777.72 1076.42L4763 1076.79L4748.28 1076.42L4733.59 1075.34L4718.98 1073.54L4704.47 1071.02L4690.11 1067.8L4675.91 1063.87L4661.93 1059.25L4648.19 1053.95L4634.73 1047.98L4621.58 1041.36L4608.77 1034.1L4596.33 1026.23L4584.29 1017.75L4572.68 1008.69L4561.53 999.07L4550.87 988.92L4540.71 978.25L4531.1 967.1L4522.04 955.5L4513.56 943.46L4505.68 931.02L4498.42 918.2L4491.8 905.05L4485.84 891.59L4480.54 877.85L4475.92 863.87L4471.99 849.68L4468.76 835.31L4466.25 820.8L4464.44 806.19L4463.36 791.51L4463 776.79L4636.29 776.79L4636.44 783.89L4636.9 790.97L4637.66 798.02L4638.72 805.02L4640.08 811.95L4641.74 818.79L4643.69 825.54L4645.93 832.17L4648.45 838.66L4651.25 845L4654.31 851.18L4657.64 857.18L4661.22 862.99L4665.05 868.59L4669.11 873.97L4673.4 879.11L4677.9 884.01L4682.61 888.65L4687.52 893.02L4692.6 897.11L4697.86 900.91L4703.27 904.41L4708.82 907.61L4714.51 910.48L4720.31 913.04L4726.22 915.27L4732.21 917.16L4738.28 918.72L4744.41 919.93L4750.58 920.8L4756.78 921.33L4763 921.5L4769.22 921.33L4775.42 920.8L4781.59 919.93L4787.72 918.72L4793.79 917.16L4799.78 915.27L4805.69 913.04L4811.49 910.48L4817.18 907.61L4822.73 904.41L4828.14 900.91L4833.4 897.11L4838.48 893.02L4843.39 888.65L4848.1 884.01L4852.6 879.11L4856.89 873.97L4860.95 868.59L4864.78 862.99L4868.36 857.18L4871.69 851.18L4874.75 845L4877.55 838.66L4880.07 832.17L4882.31 825.54L4884.26 818.79L4885.92 811.95L4887.28 805.02L4888.34 798.02L4889.1 790.97L4889.56 783.89L4889.71 776.79L4889.71 55.36ZM5473.07 798.21H6001.64L5969.5 656.07H5505.21ZM5734.5 338.21 5893.78 709.29 5897 751.07 6032.71 1055.36H6223.78L5734.5 0L5245.21 1055.36H5436.28L5574.86 742.14L5577.36 705ZM6597.87 446.43 6902.51 962.5 7207.15 446.43 7275.72 1055.36H7450.72L7313.22 5.36L6902.51 683.57L6491.8 5.36L6354.3 1055.36H6529.3ZM7790.35 1055.36H8294.63V907.14H7790.35ZM7790.35 203.57H8294.63V55.36H7790.35ZM7790.35 592.86H8266.06V446.43H7790.35ZM7685.71 55.36V1055.36H7851.78V55.36ZM8559.37 733.21 8433.65 821.07Q8466.87 889.29 8522.76 948.04Q8578.65 1006.79 8653.65 1041.79Q8728.65 1076.79 8816.51 1076.79Q8880.8 1076.79 8940.26 1056.43Q8999.72 1036.07 9046.87 997.86Q9094.01 959.64 9121.33 904.11Q9148.65 848.57 9148.65 778.57Q9148.65 712.5 9126.15 663.57Q9103.65 614.64 9066.87 578.75Q9030.08 542.86 8985.08 518.39Q8940.08 493.93 8895.08 477.86Q8815.08 450 8764.9 422.86Q8714.72 395.71 8691.33 364.64Q8667.94 333.57 8667.94 295Q8667.94 252.5 8700.97 220.18Q8734.01 187.86 8803.3 187.86Q8854.01 187.86 8891.69 207.5Q8929.37 227.14 8957.76 259.11Q8986.15 291.07 9005.8 327.86L9142.22 250.71Q9117.22 198.21 9072.05 149.46Q9026.87 100.71 8961.15 69.11Q8895.44 37.5 8806.15 37.5Q8716.87 37.5 8645.8 71.25Q8574.72 105 8533.65 165.71Q8492.58 226.43 8492.58 306.79Q8492.58 377.14 8519.37 426.43Q8546.15 475.71 8587.76 509.46Q8629.37 543.21 8675.26 564.82Q8721.15 586.43 8757.94 599.64Q8821.15 622.5 8869.19 645.89Q8917.22 669.29 8944.19 702.86Q8971.15 736.43 8971.15 791.79Q8971.15 851.43 8928.3 888.39Q8885.44 925.36 8816.15 925.36Q8760.08 925.36 8713.83 901.43Q8667.58 877.5 8629.9 834.11Q8592.22 790.71 8559.37 733.21ZM10360.17 55.36 10126.96 483.21 9894.82 55.36H9701.96L10041.96 635.36V1055.36H10213.39V633.57L10553.03 55.36ZM10861.69 555.36Q10861.69 452.14 10905.44 371.96Q10949.19 291.79 11025.98 245.54Q11102.76 199.29 11201.69 199.29Q11302.76 199.29 11378.84 245.54Q11454.91 291.79 11498.3 371.96Q11541.69 452.14 11541.69 555.36Q11541.69 658.21 11497.94 738.57Q11454.19 818.93 11377.94 865.18Q11301.69 911.43 11201.69 911.43Q11102.76 911.43 11025.98 865.18Q10949.19 818.93 10905.44 738.57Q10861.69 658.21 10861.69 555.36ZM10682.05 555.36Q10682.05 666.79 10720.62 762.32Q10759.19 857.86 10829.19 928.04Q10899.19 998.21 10994.37 1037.5Q11089.55 1076.79 11201.69 1076.79Q11314.91 1076.79 11409.55 1037.5Q11504.19 998.21 11574.19 928.04Q11644.19 857.86 11682.76 762.32Q11721.34 666.79 11721.34 555.36Q11721.34 442.86 11682.59 348.39Q11643.84 253.93 11573.48 184.46Q11503.12 115 11408.66 76.25Q11314.19 37.5 11201.69 37.5Q11090.98 37.5 10995.98 76.25Q10900.98 115 10830.62 184.46Q10760.26 253.93 10721.16 348.39Q10682.05 442.86 10682.05 555.36ZM12688.55 55.36V726.79L11958.2 5.36V1055.36H12124.27V383.93L12854.63 1105.36V55.36ZM13625.73 675H13931.8Q13918.95 725.71 13893.41 770Q13867.87 814.29 13829.48 848.21Q13791.09 882.14 13738.05 900.89Q13685.02 919.64 13618.59 919.64Q13520.02 919.64 13441.27 873.93Q13362.52 828.21 13316.45 746.07Q13270.37 663.93 13270.37 555.36Q13270.37 445.36 13317.16 364.82Q13363.95 284.29 13442.87 239.46Q13521.8 194.64 13618.59 194.64Q13722.16 194.64 13795.73 237.32Q13869.3 280 13911.09 351.79L14048.59 260Q14003.95 190 13940.73 140.54Q13877.52 91.07 13797.16 64.29Q13716.8 37.5 13618.59 37.5Q13505.73 37.5 13409.48 75Q13313.23 112.5 13241.98 181.61Q13170.73 250.71 13131.27 345.89Q13091.8 441.07 13091.8 555.36Q13091.8 669.64 13130.73 765.36Q13169.66 861.07 13240.2 930.89Q13310.73 1000.71 13406.45 1038.75Q13502.16 1076.79 13615.02 1076.79Q13734.66 1076.79 13827.7 1033.39Q13920.73 990 13983.95 914.29Q14047.16 838.57 14078.41 740.54Q14109.66 642.5 14106.8 532.14H13625.73Z","box":[0,0,14107.04,1105.36],"baseline":1055.357142857143,"capline":55.35714285714289,"square":1000,"gap":1000,"nameX":4463},"stacked":{"d":"M0 552a531 531 0 1 1 1062 0a531 531 0 1 1 -1062 0ZM1122 1062L2285 1062L1703.5 0ZM2463 1052L3463 1052L3463 52L2463 52ZM215.45 1442.08L215.45 1701.13L215.32 1706.42L214.93 1711.69L214.28 1716.94L213.38 1722.15L212.22 1727.31L210.81 1732.4L209.15 1737.42L207.25 1742.36L205.11 1747.19L202.73 1751.91L200.12 1756.52L197.29 1760.98L194.25 1765.31L191 1769.47L187.54 1773.48L183.9 1777.31L180.07 1780.95L176.06 1784.41L171.9 1787.66L167.57 1790.7L163.11 1793.53L158.51 1796.14L153.78 1798.52L148.95 1800.66L144.02 1802.56L139 1804.22L133.9 1805.63L128.74 1806.79L123.53 1807.69L118.28 1808.34L113.01 1808.73L107.72 1808.86L102.44 1808.73L97.17 1808.34L91.92 1807.69L86.71 1806.79L81.55 1805.63L76.45 1804.22L71.43 1802.56L66.5 1800.66L61.67 1798.52L56.94 1796.14L52.34 1793.53L47.88 1790.7L43.55 1787.66L39.38 1784.41L35.38 1780.95L31.55 1777.31L27.91 1773.48L24.45 1769.47L21.2 1765.31L18.15 1760.98L15.33 1756.52L12.72 1751.91L10.34 1747.19L8.2 1742.36L6.3 1737.42L4.64 1732.4L3.23 1727.31L2.07 1722.15L1.17 1716.94L0.52 1711.69L0.13 1706.42L0 1701.13L62.22 1701.13L62.28 1703.68L62.44 1706.23L62.72 1708.76L63.1 1711.27L63.59 1713.76L64.18 1716.22L64.88 1718.64L65.69 1721.02L66.59 1723.35L67.6 1725.63L68.7 1727.85L69.89 1730L71.18 1732.09L72.55 1734.1L74.01 1736.03L75.55 1737.88L77.17 1739.64L78.86 1741.3L80.62 1742.87L82.45 1744.34L84.33 1745.7L86.28 1746.96L88.27 1748.11L90.31 1749.14L92.4 1750.06L94.52 1750.86L96.67 1751.54L98.85 1752.1L101.05 1752.54L103.26 1752.85L105.49 1753.04L107.72 1753.1L109.96 1753.04L112.18 1752.85L114.4 1752.54L116.6 1752.1L118.78 1751.54L120.93 1750.86L123.05 1750.06L125.14 1749.14L127.18 1748.11L129.17 1746.96L131.12 1745.7L133 1744.34L134.83 1742.87L136.59 1741.3L138.28 1739.64L139.9 1737.88L141.44 1736.03L142.9 1734.1L144.27 1732.09L145.56 1730L146.75 1727.85L147.85 1725.63L148.86 1723.35L149.76 1721.02L150.57 1718.64L151.27 1716.22L151.86 1713.76L152.35 1711.27L152.73 1708.76L153.01 1706.23L153.17 1703.68L153.23 1701.13L153.23 1442.08ZM362.7 1708.83H552.5L540.96 1657.79H374.24ZM456.57 1543.65 513.77 1676.9 514.92 1691.9 563.66 1801.16H632.27L456.57 1422.2L280.88 1801.16H349.49L399.25 1688.69L400.14 1675.36ZM766.59 1582.51 875.98 1767.82 985.38 1582.51 1010 1801.16H1072.84L1023.46 1424.13L875.98 1667.66L728.5 1424.13L679.13 1801.16H741.97ZM1194.79 1801.16H1375.87V1747.94H1194.79ZM1194.79 1495.3H1375.87V1442.08H1194.79ZM1194.79 1635.09H1365.61V1582.51H1194.79ZM1157.22 1442.08V1801.16H1216.85V1442.08ZM1470.93 1685.49 1425.79 1717.04Q1437.72 1741.53 1457.79 1762.63Q1477.86 1783.72 1504.79 1796.29Q1531.72 1808.86 1563.27 1808.86Q1586.35 1808.86 1607.7 1801.55Q1629.05 1794.24 1645.98 1780.52Q1662.91 1766.79 1672.72 1746.85Q1682.53 1726.91 1682.53 1701.77Q1682.53 1678.05 1674.45 1660.48Q1666.37 1642.91 1653.16 1630.02Q1639.96 1617.13 1623.8 1608.35Q1607.64 1599.56 1591.48 1593.79Q1562.75 1583.79 1544.73 1574.04Q1526.72 1564.3 1518.32 1553.14Q1509.92 1541.98 1509.92 1528.13Q1509.92 1512.87 1521.78 1501.27Q1533.64 1489.66 1558.52 1489.66Q1576.73 1489.66 1590.26 1496.71Q1603.79 1503.77 1613.99 1515.24Q1624.18 1526.72 1631.24 1539.93L1680.22 1512.23Q1671.25 1493.38 1655.02 1475.87Q1638.8 1458.37 1615.2 1447.02Q1591.61 1435.67 1559.55 1435.67Q1527.49 1435.67 1501.97 1447.79Q1476.45 1459.91 1461.7 1481.71Q1446.95 1503.51 1446.95 1532.37Q1446.95 1557.63 1456.57 1575.33Q1466.19 1593.02 1481.13 1605.14Q1496.07 1617.26 1512.55 1625.02Q1529.02 1632.78 1542.23 1637.52Q1564.93 1645.73 1582.18 1654.13Q1599.43 1662.53 1609.11 1674.59Q1618.8 1686.64 1618.8 1706.52Q1618.8 1727.94 1603.41 1741.21Q1588.02 1754.48 1563.14 1754.48Q1543 1754.48 1526.4 1745.89Q1509.79 1737.3 1496.26 1721.72Q1482.73 1706.14 1470.93 1685.49ZM2117.57 1442.08 2033.82 1595.72 1950.47 1442.08H1881.21L2003.3 1650.35V1801.16H2064.86V1649.71L2186.82 1442.08ZM2297.65 1621.62Q2297.65 1584.56 2313.36 1555.77Q2329.07 1526.98 2356.65 1510.37Q2384.22 1493.76 2419.74 1493.76Q2456.03 1493.76 2483.35 1510.37Q2510.67 1526.98 2526.25 1555.77Q2541.83 1584.56 2541.83 1621.62Q2541.83 1658.56 2526.12 1687.41Q2510.41 1716.27 2483.03 1732.87Q2455.65 1749.48 2419.74 1749.48Q2384.22 1749.48 2356.65 1732.87Q2329.07 1716.27 2313.36 1687.41Q2297.65 1658.56 2297.65 1621.62ZM2233.15 1621.62Q2233.15 1661.63 2247 1695.94Q2260.85 1730.24 2285.98 1755.44Q2311.12 1780.64 2345.3 1794.75Q2379.47 1808.86 2419.74 1808.86Q2460.4 1808.86 2494.38 1794.75Q2528.36 1780.64 2553.5 1755.44Q2578.64 1730.24 2592.49 1695.94Q2606.34 1661.63 2606.34 1621.62Q2606.34 1581.23 2592.42 1547.31Q2578.51 1513.39 2553.24 1488.44Q2527.98 1463.5 2494.06 1449.58Q2460.14 1435.67 2419.74 1435.67Q2379.99 1435.67 2345.87 1449.58Q2311.76 1463.5 2286.5 1488.44Q2261.23 1513.39 2247.19 1547.31Q2233.15 1581.23 2233.15 1621.62ZM2953.65 1442.08V1683.18L2691.39 1424.13V1801.16H2751.02V1560.07L3013.28 1819.12V1442.08ZM3290.17 1664.58H3400.07Q3395.46 1682.79 3386.29 1698.7Q3377.12 1714.6 3363.33 1726.78Q3349.55 1738.97 3330.5 1745.7Q3311.46 1752.43 3287.61 1752.43Q3252.21 1752.43 3223.93 1736.02Q3195.65 1719.6 3179.11 1690.1Q3162.57 1660.61 3162.57 1621.62Q3162.57 1582.12 3179.37 1553.2Q3196.17 1524.29 3224.51 1508.19Q3252.85 1492.1 3287.61 1492.1Q3324.8 1492.1 3351.21 1507.42Q3377.63 1522.75 3392.64 1548.52L3442.01 1515.57Q3425.98 1490.43 3403.28 1472.67Q3380.58 1454.91 3351.73 1445.29Q3322.87 1435.67 3287.61 1435.67Q3247.08 1435.67 3212.52 1449.14Q3177.96 1462.6 3152.37 1487.42Q3126.79 1512.23 3112.62 1546.41Q3098.45 1580.58 3098.45 1621.62Q3098.45 1662.66 3112.42 1697.03Q3126.4 1731.4 3151.73 1756.47Q3177.06 1781.54 3211.43 1795.2Q3245.8 1808.86 3286.32 1808.86Q3329.28 1808.86 3362.69 1793.28Q3396.1 1777.7 3418.8 1750.51Q3441.5 1723.32 3452.72 1688.12Q3463.94 1652.91 3462.91 1613.29H3290.17Z","box":[0,0,3463,1819.12],"baseline":1052,"capline":52,"square":1000,"nameCap":359.08179197151196,"nameBaseline":1801.1635839430237,"gap":359.08179197151196},"params":{"D":1062,"cy":0,"b":1163,"h":1062,"ty":-10,"g1":60,"g2":178,"alpha":0.5,"depth":125,"square":1000,"order":"circle, triangle, square","whitePerGap":[238.4,290.0],"perceived":{"circle":99.98,"triangle":93.45,"square":100.0}}};

  function cx() { var out = []; for (var i = 0; i < arguments.length; i++) if (arguments[i]) out.push(arguments[i]); return out.join(' '); }
  function r2(v) { return Math.round(v * 100) / 100; }
  function num(v) { var s = String(Math.round(v)); return s.charAt(0) === '-' ? '\u2212' + s.slice(1) : s; }
  function assign(a, b) { var o = {}, k; for (k in a) o[k] = a[k]; for (k in b) o[k] = b[k]; return o; }
  function omit(p, keys) { var o = {}; for (var k in p) if (keys.indexOf(k) < 0) o[k] = p[k]; return o; }

  function useReducedMotion() {
    var q = typeof window !== 'undefined' && window.matchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)') : null;
    var st = useState(q ? q.matches : false);
    useEffect(function () {
      if (!q) return undefined;
      var on = function () { st[1](q.matches); };
      if (q.addEventListener) q.addEventListener('change', on); else q.addListener(on);
      return function () { if (q.removeEventListener) q.removeEventListener('change', on); else q.removeListener(on); };
    }, []);
    return st[0];
  }

  var FORMS = { row: GEO.row, wrap: GEO.wrap, micro: GEO.wrapMicro };

  /* ---------- construction overlay for the mark (drawn behind the shapes) ---------- */
  function line(x1, y1, x2, y2, key, cls) {
    return h('line', { key: key, x1: r2(x1), y1: r2(y1), x2: r2(x2), y2: r2(y2), className: cls || 'jy-c-line', vectorEffect: 'non-scaling-stroke' });
  }
  function dim(xa, xb, y, label, k, key, ya, yb) {
    /* horizontal dimension line from xa to xb at y, extension lines up to ya / yb, label above the line */
    var tick = 6 / k;
    var els = [
      line(xa, y, xb, y, key + 'l'),
      line(xa, y - tick, xa, y + tick, key + 'a'),
      line(xb, y - tick, xb, y + tick, key + 'b')
    ];
    if (ya != null) els.push(line(xa, ya, xa, y, key + 'ea', 'jy-c-line jy-c-ext'));
    if (yb != null) els.push(line(xb, yb, xb, y, key + 'eb', 'jy-c-line jy-c-ext'));
    if (label != null) els.push(h('text', { key: key + 't', x: r2((xa + xb) / 2), y: r2(y + 16 / k), className: 'jy-c-label', fontSize: r2(11 / k), textAnchor: 'middle' }, label));
    return els;
  }
  function vdim(x, ya, yb, label, k, key) {
    var tick = 6 / k;
    var els = [line(x, ya, x, yb, key + 'l'), line(x - tick, ya, x + tick, ya, key + 'a'), line(x - tick, yb, x + tick, yb, key + 'b')];
    if (label != null) els.push(h('text', { key: key + 't', x: r2(x - 8 / k), y: r2((ya + yb) / 2 + 4 / k), className: 'jy-c-label', fontSize: r2(11 / k), textAnchor: 'end' }, label));
    return els;
  }
  function Construction(p) {
    var g = p.g, k = p.k, vb = p.vb, labels = p.size >= 64, els = [];
    var X0 = vb[0], X1 = vb[0] + vb[2];
    var bases = p.layout === 'row' ? [g.baseline] : [g.baseline1, g.baseline];
    bases.forEach(function (b, i) {
      els.push(line(X0, b, X1, b, 'b' + i, 'jy-c-line jy-c-main'));
      els.push(line(X0, b - 1000, X1, b - 1000, 'c' + i, 'jy-c-line jy-c-main'));
    });
    g.parts.forEach(function (pt, i) {
      var bx = pt.box;
      els.push(h('rect', { key: 'r' + i, x: bx[0], y: bx[1], width: r2(bx[2] - bx[0]), height: r2(bx[3] - bx[1]), className: 'jy-c-box', vectorEffect: 'non-scaling-stroke' }));
      if (pt.kind === 'c') {
        var ccx = (bx[0] + bx[2]) / 2;
        els.push(line(ccx, bx[1] - 40, ccx, bx[3] + 40, 'ax' + i, 'jy-c-line jy-c-ext'));
      }
      if (pt.kind === 't') {
        var ax = (bx[0] + bx[2]) / 2;
        els.push(line(ax, bx[1] - 40, ax, bx[3] + 40, 'ax' + i, 'jy-c-line jy-c-ext'));
      }
    });
    if (labels) {
      var P = g.parts;
      if (p.layout === 'row') {
        var y = g.baseline + 150;
        els = els.concat(dim(P[0].box[2], P[1].box[0], y, String(Math.round(P[1].box[0] - P[0].box[2])), k, 'g1', P[0].box[1] + 531, P[1].box[3]));
        els = els.concat(dim(P[1].box[2], P[2].box[0], y, String(Math.round(P[2].box[0] - P[1].box[2])), k, 'g2', P[1].box[3], P[2].box[3]));
        els.push(h('text', { key: 'lc', x: r2(X0 + 6 / k), y: r2(g.baseline - 1000 - 6 / k), className: 'jy-c-label', fontSize: r2(11 / k) }, 'cap 1000'));
        els.push(h('text', { key: 'lb', x: r2(X0 + 6 / k), y: r2(g.baseline + 15 / k), className: 'jy-c-label', fontSize: r2(11 / k) }, 'base 0'));
        els.push(h('text', { key: 'lo', x: r2(X1 - 6 / k), y: r2(P[1].box[1] - 8 / k), className: 'jy-c-label', fontSize: r2(11 / k), textAnchor: 'end' }, '\u25B3 +' + Math.round(g.baseline - 1000 - P[1].box[1]) + '  \u25CB +' + Math.round(g.baseline - 1000 - P[0].box[1])));
      } else {
        els = els.concat(vdim(P[2].box[0] - 90, P[0].box[3], P[2].box[1], String(Math.round(P[2].box[1] - P[0].box[3])), k, 'rg'));
        els.push(h('text', { key: 'lg', x: r2(X1 - 6 / k), y: r2(P[1].box[1] - 8 / k), className: 'jy-c-label', fontSize: r2(11 / k), textAnchor: 'end' }, 'gap ' + Math.round(P[1].box[0] - P[0].box[2])));
      }
    }
    return h('g', { className: 'jy-construction', 'aria-hidden': 'true' }, els);
  }

  /* ---------- Mark ---------- */
  function Mark(props) {
    var layout = FORMS[props.layout] ? props.layout : 'row';
    var g = FORMS[layout];
    var size = props.size == null ? 24 : Number(props.size);
    var k = size / 1000;
    var pad = props.construction ? (size >= 64 ? 320 : 140) : 0;
    var padB = props.construction && size >= 64 && layout === 'row' ? Math.max(pad, 150 + 26000 / size) : pad;
    var b = g.box;
    var vb = [b[0] - pad, b[1] - pad, (b[2] - b[0]) + 2 * pad, (b[3] - b[1]) + pad + padB];
    var reduced = useReducedMotion();
    var reveal = props.reveal && !reduced;
    var title = props.title || 'James Yong';
    var rest = omit(props, ['layout', 'size', 'construction', 'reveal', 'title', 'color', 'className']);
    return h('svg', assign(rest, {
      className: cx('jy-mark', reveal && 'jy-mark--reveal', props.construction && 'jy-mark--construction', props.className),
      viewBox: vb.map(r2).join(' '), width: r2(vb[2] * k), height: r2(vb[3] * k),
      role: 'img', 'aria-label': title, focusable: 'false'
    }),
      props.construction ? h(Construction, { g: g, layout: layout, size: size, k: k, vb: vb }) : null,
      g.parts.map(function (pt, i) {
        return h('path', { key: i, d: pt.d, className: 'jy-mark-part', fill: props.color || 'currentColor', style: reveal ? { animationDelay: (i * 90) + 'ms' } : undefined });
      })
    );
  }

  /* ---------- Wordmark and Lockup ---------- */
  function Outline(g, size, unitsPerPx, props, extraClass) {
    var k = size / unitsPerPx;
    var b = g.box, title = props.title || 'James Yong';
    var rest = omit(props, ['layout', 'size', 'title', 'color', 'className']);
    return h('svg', assign(rest, {
      className: cx(extraClass, props.className), viewBox: b.map(r2).join(' '),
      width: r2((b[2] - b[0]) * k), height: r2((b[3] - b[1]) * k), role: 'img', 'aria-label': title, focusable: 'false'
    }), h('path', { d: g.d, fill: props.color || 'currentColor' }));
  }
  function Wordmark(props) { return Outline(GEO.wordmark, props.size == null ? 16 : Number(props.size), 1000, props, 'jy-wordmark'); }
  function Lockup(props) {
    var layout = props.layout === 'stacked' ? 'stacked' : 'inline';
    return Outline(GEO[layout], props.size == null ? 24 : Number(props.size), 1000, props, 'jy-lockup jy-lockup--' + layout);
  }

  /* ---------- Loader ---------- */
  function Loader(props) {
    var size = props.size == null ? 12 : Number(props.size), k = size / 1000, g = GEO.row, b = g.box;
    var label = props.label || 'Loading';
    return h('span', { className: cx('jy-loader', props.className), role: 'status', 'aria-live': 'polite', style: props.style },
      h('svg', { viewBox: b.join(' '), width: r2((b[2] - b[0]) * k), height: r2((b[3] - b[1]) * k), 'aria-hidden': 'true', focusable: 'false' },
        g.parts.map(function (pt, i) { return h('path', { key: i, d: pt.d, className: 'jy-loader-part', fill: 'currentColor' }); })),
      h('span', { className: 'jy-visually-hidden' }, label));
  }

  /* ---------- Measure: a dimension line ---------- */
  function Measure(props) {
    var axis = props.axis === 'y' ? 'y' : 'x';
    var len = Math.max(8, Number(props.length) || 0);
    var label = props.label != null ? props.label : String(Math.round(len));
    var W = axis === 'x' ? len : 28, H = axis === 'x' ? 22 : len;
    var lines = axis === 'x'
      ? [[0.5, 15, len - 0.5, 15], [0.5, 10, 0.5, 20], [len - 0.5, 10, len - 0.5, 20]]
      : [[14, 0.5, 14, len - 0.5], [9, 0.5, 19, 0.5], [9, len - 0.5, 19, len - 0.5]];
    return h('span', { className: cx('jy-measure', 'jy-measure--' + axis, props.className), style: assign({ width: W + 'px', height: H + 'px' }, props.style), role: 'img', 'aria-label': (props.title || 'Measures') + ' ' + label },
      h('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, 'aria-hidden': 'true' },
        lines.map(function (l, i) { return h('line', { key: i, x1: l[0], y1: l[1], x2: l[2], y2: l[3] }); })),
      h('span', { className: 'jy-measure-label' }, label));
  }

  /* ---------- Blueprint: hold Option (Alt) to see how the thing is built ---------- */
  function Blueprint(props) {
    var ref = useRef(null);
    var held = useState(false), on = useState(!!props.defaultShow), m = useState(null);
    var active = props.show != null ? !!props.show : (on[0] || held[0]);
    var columns = props.columns == null ? 12 : props.columns, gutter = props.gutter == null ? 24 : props.gutter, margin = props.margin == null ? 0 : props.margin;

    useEffect(function () {
      if (props.hotkey === false) return undefined;
      function down(e) { if (e.key === 'Alt') held[1](true); }
      function up(e) { if (e.key === 'Alt') held[1](false); }
      function reset() { held[1](false); }
      window.addEventListener('keydown', down); window.addEventListener('keyup', up); window.addEventListener('blur', reset);
      return function () { window.removeEventListener('keydown', down); window.removeEventListener('keyup', up); window.removeEventListener('blur', reset); };
    }, [props.hotkey]);

    useLayoutEffect(function () {
      if (!active || !ref.current) return undefined;
      var el = ref.current, raf = 0;
      function measure() {
        var r = el.getBoundingClientRect();
        var list = Array.prototype.slice.call(el.querySelectorAll('[data-measure]'));
        if (!list.length) list = Array.prototype.filter.call(el.children, function (c) { return !c.hasAttribute('data-jy-layer'); });
        var boxes = list.map(function (t) {
          var q = t.getBoundingClientRect();
          return { x: q.left - r.left, y: q.top - r.top, w: q.width, h: q.height, label: t.getAttribute('data-measure') || '' };
        }).filter(function (bx) { return bx.w > 0 && bx.h > 0; });
        var sorted = boxes.slice().sort(function (a, b) { return a.y - b.y || a.x - b.x; });
        var gaps = [];
        for (var i = 0; i < sorted.length; i++) {
          for (var j = i + 1; j < sorted.length; j++) {
            var A = sorted[i], B = sorted[j];
            var ox0 = Math.max(A.x, B.x), ox1 = Math.min(A.x + A.w, B.x + B.w);
            if (ox1 - ox0 > 8 && B.y > A.y + A.h + 0.5) { gaps.push({ x: ox0 + Math.min(16, (ox1 - ox0) / 2), y0: A.y + A.h, y1: B.y }); break; }
          }
        }
        m[1]({ w: r.width, h: r.height, boxes: boxes, gaps: gaps.slice(0, 24) });
      }
      function schedule() { cancelAnimationFrame(raf); raf = requestAnimationFrame(measure); }
      measure();
      var ro = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(schedule) : null;
      if (ro) ro.observe(el);
      window.addEventListener('resize', schedule);
      return function () { cancelAnimationFrame(raf); if (ro) ro.disconnect(); window.removeEventListener('resize', schedule); };
    }, [active, columns, gutter, margin]);

    var layer = null, M = m[0];
    if (active && M) {
      var els = [], inner = M.w - 2 * margin, colW = (inner - (columns - 1) * gutter) / columns;
      if (columns > 0 && colW > 4) for (var c = 0; c < columns; c++) {
        els.push(h('rect', { key: 'col' + c, x: r2(margin + c * (colW + gutter)), y: 0, width: r2(colW), height: r2(M.h), className: 'jy-bp-col' }));
      }
      M.boxes.forEach(function (bx, i) {
        els.push(h('rect', { key: 'b' + i, x: r2(bx.x + 0.5), y: r2(bx.y + 0.5), width: r2(Math.max(0, bx.w - 1)), height: r2(Math.max(0, bx.h - 1)), className: 'jy-bp-box' }));
        if (bx.label || bx.w >= 120) {
          var txt = (bx.label ? bx.label + '  ' : '') + Math.round(bx.w) + ' \u00D7 ' + Math.round(bx.h);
          els.push(h('text', { key: 't' + i, x: r2(bx.x + bx.w - 2), y: r2(Math.max(11, bx.y - 4)), className: 'jy-bp-label', textAnchor: 'end' }, txt));
        }
      });
      M.gaps.forEach(function (gp, i) {
        var x = r2(gp.x), y0 = r2(gp.y0), y1 = r2(gp.y1);
        els.push(h('line', { key: 'g' + i, x1: x, y1: y0, x2: x, y2: y1, className: 'jy-bp-dim' }));
        els.push(h('line', { key: 'ga' + i, x1: x - 4, y1: y0, x2: x + 4, y2: y0, className: 'jy-bp-dim' }));
        els.push(h('line', { key: 'gb' + i, x1: x - 4, y1: y1, x2: x + 4, y2: y1, className: 'jy-bp-dim' }));
        if (gp.y1 - gp.y0 >= 10) els.push(h('text', { key: 'gt' + i, x: x + 6, y: r2((gp.y0 + gp.y1) / 2 + 4), className: 'jy-bp-label' }, String(Math.round(gp.y1 - gp.y0))));
      });
      var cols = els.filter(function (e) { return String(e.key).indexOf('col') === 0; });
      var marks = els.filter(function (e) { return String(e.key).indexOf('col') !== 0; });
      layer = [
        h('svg', { key: 'grid', className: 'jy-bp-grid', width: r2(M.w), height: r2(M.h), 'aria-hidden': 'true', 'data-jy-layer': '' }, cols),
        h('svg', { key: 'marks', className: 'jy-bp-layer', width: r2(M.w), height: r2(M.h), 'aria-hidden': 'true', 'data-jy-layer': '' }, marks)
      ];
    }
    var Tag = props.as || 'div';
    var rest = omit(props, ['show', 'defaultShow', 'hotkey', 'columns', 'gutter', 'margin', 'label', 'as', 'className', 'children', 'toggle']);
    return h(Tag, assign(rest, { ref: ref, className: cx('jy-blueprint', active && 'is-active', props.className) }),
      props.children,
      layer,
      props.toggle ? h('button', {
        type: 'button', className: 'jy-bp-toggle', 'aria-pressed': active ? 'true' : 'false', 'data-jy-layer': '',
        onClick: function () { on[1](!on[0]); }
      }, h('span', { className: 'jy-bp-key', 'aria-hidden': 'true' }, '\u2325'), ' ', props.label || 'Construction') : null
    );
  }

  /* ---------- Interface ---------- */
  function Button(props) {
    var variant = props.variant || 'primary', size = props.size || 'md';
    var cls = cx('jy-btn', 'jy-btn--' + variant, 'jy-btn--' + size, props.className);
    var rest = omit(props, ['variant', 'size', 'className', 'children']);
    if (props.href) return h('a', assign(rest, { className: cls }), props.children);
    return h('button', assign({ type: 'button' }, assign(rest, { className: cls })), props.children);
  }

  function TextLink(props) {
    var rest = omit(props, ['external', 'className', 'children']);
    var ext = props.external ? { target: '_blank', rel: 'noreferrer' } : {};
    return h('a', assign(assign(rest, ext), { className: cx('jy-link', props.className) }), props.children,
      props.external ? h('span', { className: 'jy-link-ext', 'aria-hidden': 'true' }, '\u00A0\u2197') : null);
  }

  var GLYPH = { circle: 0, triangle: 1, square: 2 };
  function ShapeGlyph(props) {
    var pt = GEO.row.parts[GLYPH[props.shape]];
    if (!pt) return null;
    var x0 = pt.box[0], x1 = pt.box[2];
    return h('svg', { className: 'jy-glyph', viewBox: r2(x0) + ' 0 ' + r2(x1 - x0) + ' 1083', 'aria-hidden': 'true', focusable: 'false' },
      h('path', { d: pt.d, fill: 'currentColor' }));
  }

  function Tag(props) {
    var El = props.as || 'span';
    var rest = omit(props, ['shape', 'as', 'className', 'children']);
    return h(El, assign(rest, { className: cx('jy-tag', props.className) }),
      props.shape ? h(ShapeGlyph, { shape: props.shape }) : null,
      h('span', { className: 'jy-tag-text' }, props.children));
  }

  function Kbd(props) { return h('kbd', { className: cx('jy-kbd', props.className) }, props.children); }

  /* Icons: 24px grid, 1.5px stroke, square caps, mitred joins (the square's corners, the circle's curves) */
  var ICONS = {"icons": {"arrow-right": "M4 12H19M13 6L19 12L13 18", "arrow-left": "M20 12H5M11 6L5 12L11 18", "arrow-up-right": "M7 17L17 7M9 7H17V15", "download": "M12 3V15M7 10L12 15L17 10M4 20H20", "close": "M6 6L18 18M18 6L6 18", "menu": "M4 9H20M4 15H20", "plus": "M12 5V19M5 12H19", "minus": "M5 12H19", "check": "M5 12.5L10 17.5L19 7", "copy": "M9 9H20V20H9ZM15 9V4H4V15H9", "search": "M15 15L20 20M16.5 10.5A6 6 0 1 1 4.5 10.5A6 6 0 1 1 16.5 10.5Z", "mail": "M3 6H21V18H3ZM3 6L12 13L21 6", "theme": "M20 12A8 8 0 1 1 4 12A8 8 0 1 1 20 12Z"}, "fills": {"theme": "M12 4A8 8 0 0 0 12 20Z"}};
  function Icon(props) {
    var d = ICONS.icons[props.name];
    if (!d) return null;
    var size = props.size == null ? 24 : Number(props.size), fill = ICONS.fills[props.name], label = props.title;
    return h('svg', {
      className: cx('jy-icon', props.className), viewBox: '0 0 24 24', width: size, height: size, fill: 'none', stroke: 'currentColor',
      strokeWidth: 1.5, strokeLinecap: 'square', strokeLinejoin: 'miter', role: label ? 'img' : undefined, 'aria-label': label || undefined,
      'aria-hidden': label ? undefined : 'true', focusable: 'false', style: props.style
    }, h('path', { d: d }), fill ? h('path', { d: fill, fill: 'currentColor', stroke: 'none' }) : null);
  }

  function ProjectCard(props) {
    var El = props.href ? 'a' : 'article';
    var rest = omit(props, ['title', 'summary', 'year', 'tags', 'media', 'mediaRatio', 'className', 'href', 'headingLevel']);
    var H = 'h' + (props.headingLevel || 3);
    return h(El, assign(rest, { href: props.href, className: cx('jy-card', props.href && 'jy-card--link', props.className) }),
      h('div', { className: 'jy-card-media', style: { aspectRatio: props.mediaRatio || '16 / 10' } },
        props.media || h('div', { className: 'jy-card-placeholder', 'aria-hidden': 'true' }, h(Mark, { size: 28, color: 'currentColor', title: '' }))),
      h('div', { className: 'jy-card-body' },
        h(H, { className: 'jy-card-title' }, props.title),
        props.summary ? h('p', { className: 'jy-card-summary' }, props.summary) : null,
        h('div', { className: 'jy-card-meta' },
          props.year ? h('span', { className: 'jy-card-year' }, props.year) : null,
          (props.tags || []).map(function (t, i) { return h(Tag, { key: i, shape: t.shape }, t.label); }))),
      h('span', { className: 'jy-card-ticks', 'aria-hidden': 'true' }, h('i'), h('i'), h('i'), h('i')));
  }

  var api = { Mark: Mark, Wordmark: Wordmark, Lockup: Lockup, Loader: Loader, Blueprint: Blueprint, Measure: Measure, Button: Button, TextLink: TextLink, Tag: Tag, Kbd: Kbd, Icon: Icon, ProjectCard: ProjectCard, geometry: GEO, icons: Object.keys(ICONS.icons) };
  window.JY = window.JY || {};
  for (var key in api) window.JY[key] = api[key];
})();
