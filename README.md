<img src="http://gblogs.cisco.com/fr-datacenter/wp-content/uploads/sites/14/2013/09/hadoop-elephant_logo.png" alt="Hadoop Logo" height="200"/>

# hadoop-regression-vehicles

This repo is an example of Hadoop Map-Combiner-Reduce operation over CSV files.

Regression is a Java program that gathers vehicle data from Vehicules.csv file, representing a vehicle sample.

The goal of this is to calculate a covariance between the weight and the consumption of the vehicle.

## File structure

The Vehicules.csv is structured like this :

Weight (kg) | Consumption (liters)
--- | ---

## Execute the project
With hadoop installed, you must put the file on the hadoop disk :

`hadoop fs -put Vehicules.csv /test`

Next, after having compiled the project (with Maven for example : `mvn clean package`), you will execute the project :

`hadoop jar NameOfYourJar.jar RegressionDriver /test/Vehicules.csv /results`

You can see the results using <img src="http://gethue.com/wp-content/uploads/2014/03/hue_logo_300dpi_huge.png" height="15"/> (Hue) for example.

## Results

X = Weight

Y = Consumption

```
n	28.0
Sx	33515.0
Sx²	4.2694125E7
X Variance	92066.67729591834
Sy	254.1
Sy²	2440.5699999999993
Sum X x Y	321404.5
X average	1196.9642857142858
X² average	1524790.1785714286
Y average	9.075
Y² average	87.16321428571426
X x Y average	11478.732142857143
X variance	92066.67729591834
Y variance	4.807589285714272
Covariance	616.28125
Corrélation r	0.9263263981866983
β0	1.0626912269314541
β1	0.006693857844127085
yi = 1.0626912269314541 + 0.006693857844127085 * xi + ei	0.0
```

The correlation between the weight and the consumption of the vehicle is very good !
