#Parse HerSNP_Alex.csv
library(dplyr)
library(reshape)

snp <- HerSNP_Alex

#remove col not to be inserted in the table 
snp <- select(snp,-c('area', 'oName', 'pop'))

#long format, has to specify id variable
snp2 <- melt(snp, id=c('ind'))

snp2 <- rename(snp2, c('Sample_id' = 'ind'))
snp2 <- rename(snp2, c('Marker_id' = 'variable'))
snp2 <- rename(snp2, c('Gt' = 'value'))

#write csv2 directly with sep ;
write.csv2(snp2, 
          'C:/Users/alexa/Matis/SQL/results/04_06_20/HerSNP_long.csv', 
          row.names = FALSE)
