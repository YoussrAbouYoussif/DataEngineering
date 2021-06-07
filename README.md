# CSEN1095-W20-Project
This a template repository for the CSEN1095 project winter 2020. It contains the data that will be used in this project. 
Good luck :)

# Overview of the used datasets
## Table of contents
* [Happiness](#happiness-dataset)
* [Life Expectancy](#life-expectancy-dataset)
* [250 Country](#250-country-dataset)
* [Population](#population-dataset)
* [GDP](#gdp-dataset)
* [65 Index](#65-index-dataset)

### Happiness Dataset
First of all, this dataset shows the happiness rank and happiness score of all the countries from years 2015 to 2019, where the happiness score is calculated as the sum of economy, family, life expectancy, freedom, trust and generosity. The used attributes from this dataset are Country, Happiness Score, Freedom, Trust and GDP.

### Life Expectancy Dataset
First of all, this dataset shows all the factors that might affect the Life Expectancy of each country. These factors are economic factors and other health related factors, taking into consideration the the important immunization such as Hepatitis B, Polio and Diphtheria. Thus, it can be used to analyze the factors that can be improved to improve the life expectancy of each country. The used attributes from this dataset are Country, Adult Mortality, Infant Deaths, Under 5Y/O Deaths, Hepatitis B, HIV/AIDS, Population and Life Expectancy.

## 250 Country Dataset
First of all, this dataset shows many factors in each country such as the region and subregion of the country, population, area literacy rate and many other factors. The used attributes from this dataset are Country, Region, Subregion, Literacy Rate(%), gini, area and Inflation(%).

## External Datasets
### Population Dataset
This dataset is used to fill the null values in the Life Expectancy dataset. This dataset is achieved from WHO.

### GDP Dataset
This dataset is used to fill the null values in the Life Expectancy dataset. This dataset is achieved from WHO.

### 65 Index Dataset
This dataset shows many factors for each country, such as male suicidal rate, female suicidal rate, green areas and many other factors. This dataset is integrated with the first three datasets to show relations between the,. The attributes used from this dataset are Country, Change forest %, Forest area %, FemaleSuicide % MaleSuicide %, Homicide %, Prison population and Gender Inequality.

# Overview of the project goals and motivation behind it
What are the different factors that affect the happiness and life expectancy of different countries. Thus, the output dataset can be used to analyze the factors that affect the happiness score and life expectancy of the countries. Also, this dataset shows how literacy rate in each country affect the crime types done in a country. Last but not least, the output dataset can be used to analyze the effect of overpopulation of the countries on many factors.

# Steps used for the work
##### 1- Reading the datasets.
##### 2- Cleaning and tidying each dataset on its own and filling the nulls.
##### 3- Merging the three given datasets (Happiness, Life Expectancy and 250 Country). Also, merging the external dataset (65 Index) with them. The merging is done the Country name.
##### 4- Coming up with the research questions that will help in giving the motivation of our work.
##### 5- Analyzing the relation between the factors of each research question and answering the research question based on this relation.
##### 6- Exploring the datasets to show all different insights.
##### 7- Pipelining the project using Airflow, to run the tasks.
  - ###### Stages:
    - ###### 1- Extract data E(Extract).
    - ###### 2- Transform data T(Transform).
    - ###### 3- Store data L(Load).


# Data exploration questions
##### 1- How does the females' and males' suicidal rates affect the happiness score of each country?
##### 2- How does the increasing of green areas affect the happiness score of each country?
##### 3- What is the effect of the deaths of different ranges of ages on the happiness score?
##### 4- What is the effect of literacy rate on crime types?
##### 5- How does the females' and males' suicidal rates affect the happiness score of each country?
##### 6- Is freedom really needed in a country?
##### 7- What is the effect of overpopulation on many aspects in the countries?


# Feature Engineering
##### 1- Where the Adult Mortality , Under 5Y/O Deaths and Infant Deaths are calculated for 1000 so to calculate for the whole Population we multiplied by 1000 and divided by population.(Interaction)
##### 2- Here we create two Features Engineering 
  - [1] Calculating total suicide rate. 
  - [2] Calculating the density of each country from population and area. 
    - Since The total suicidal rate is calculated to represent the total suicidal rates in each country per the density of the country. While, density is calculated to visualize the relation between over-population and many factors as shown in the Figures below.(Interaction) 
