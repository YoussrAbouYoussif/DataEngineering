# step 1 - import modules
import requests
import json

from airflow import DAG
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from sklearn import preprocessing


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 17)
    }


# step 3 - instantiate DAG
dag = DAG(
    'data-cleaning-and-integration',
    default_args=default_args,
    description='Reading all datasets ,Cleaning and Integrating and Storing the merged dataset',
    schedule_interval='@once',
)

def extract_data(**kwargs):
    df_happiness_2015 =  pd.read_csv('./data/Happiness_Dataset/2015.csv')
    df_happiness_2016 =  pd.read_csv('./data/Happiness_Dataset/2016.csv')
    df_happiness_2017 =  pd.read_csv('./data/Happiness_Dataset/2017.csv')
    df_happiness_2018 =  pd.read_csv('./data/Happiness_Dataset/2018.csv')
    df_happiness_2019 =  pd.read_csv('./data/Happiness_Dataset/2019.csv')
    df_250Country =  pd.read_csv('./data/250 Country Data.csv')   
    df_life_expectancy = pd.read_csv("./data/Life Expectancy Data.csv")
    df_65_index =  pd.read_csv('./data/Kaggle.csv')
    df_GDP = pd.read_csv("./data/GDP_Dataset1.csv")
    df_population = pd.read_csv("./data/Population_Dataset1.csv")

    return [df_happiness_2015,df_happiness_2016,df_happiness_2017,df_happiness_2018,df_happiness_2019,df_250Country,df_life_expectancy,df_65_index,df_GDP,df_population]


def transform_data(**context):
    df_list = context['task_instance'].xcom_pull(task_ids='extract_data')
    df_2015 = df_list[0]
    df_2016 = df_list[1]
    df_2017 = df_list[2]
    df_2018 = df_list[3]
    df_2019 = df_list[4]
    df_250Country = df_list[5]
    df_life_expectancy = df_list[6]
    df_65_Index = df_list[7]
    df_GDP = df_list[8]
    df_population = df_list[9]
    #Cleaning data
    df_2015.rename(columns = {'Economy (GDP per Capita)':'GDP', 'Health (Life Expectancy)':'Health','Trust (Government Corruption)':'Trust'}, inplace = True) 
    df_2015=df_2015.drop(["Standard Error","Region"],axis=1)

    df_2016.rename(columns = {'Economy (GDP per Capita)':'GDP', 'Health (Life Expectancy)':'Health','Trust (Government Corruption)':'Trust'}, inplace = True) 
    df_2016=df_2016.drop(["Lower Confidence Interval","Upper Confidence Interval","Region"],axis=1)


    df_2017.rename(columns = {'Economy..GDP.per.Capita.':'GDP', 'Health..Life.Expectancy.':'Health','Trust..Government.Corruption.':'Trust','Whisker.low':'Lower Confidence Interval','Whisker.high':'Upper Confidence Interval','Happiness.Rank':'Happiness Rank','Happiness.Score':'Happiness Score','Dystopia.Residual':'Dystopia Residual'}, inplace = True)
    df_2017=df_2017.drop(["Lower Confidence Interval","Upper Confidence Interval"],axis=1)

    df_2018.rename(columns = {'Country or region':'Country','Overall rank':'Happiness Rank', 'Score':'Happiness Score','Healthy life expectancy':'Health','Freedom to make life choices':'Freedom','Social support':'Family','Perceptions of corruption':'Trust','GDP per capita':'GDP'}, inplace = True)
    df_2018['Dystopia Residual']=df_2018['Happiness Score']-(df_2018['GDP']+df_2018['Family']+df_2018['Health']+df_2018['Freedom']+df_2018['Generosity']+df_2018['Trust'])

    df_2019.rename(columns = {'Country or region':'Country','Overall rank':'Happiness Rank', 'Score':'Happiness Score','Healthy life expectancy':'Health','Freedom to make life choices':'Freedom','Social support':'Family','Perceptions of corruption':'Trust','GDP per capita':'GDP'}, inplace = True)
    df_2019['Dystopia Residual']=df_2019['Happiness Score']-(df_2019['GDP']+df_2019['Family']+df_2019['Health']+df_2019['Freedom']+df_2019['Generosity']+df_2019['Trust'])

    df_all = pd.concat([df_2015, df_2016, df_2017, df_2018, df_2019])
    df_all = df_all.groupby(['Country'], as_index=False).mean()
    df_all['name'] = df_all['Country']
    df_happiness =  df_all.drop(['Country'], axis=1)

    
    df_250Country = df_250Country.drop(['Unnamed: 0'], axis=1)


    df_250Country['Unemployement(%)'] = df_250Country['Unemployement(%)'].str.extract('([-+]?\d*\.\d+|\d+)',expand=False).astype(float)
    df_250Country['Real Growth Rating(%)'] = df_250Country['Real Growth Rating(%)'].str.extract('([-+]?\d*\.\d+|\d+)',expand=False).astype(float)
    df_250Country['Literacy Rate(%)'] = df_250Country['Literacy Rate(%)'].str.extract('([-+]?\d*\.\d+|\d+)',expand=False).astype(float)
    df_250Country['Inflation(%)'] = df_250Country['Inflation(%)'].str.extract('([-+]?\d*\.\d+|\d+)',expand=False).astype(float)



    #Dropping the unwanted columns
    df_population = df_population.copy().drop(['Country Code', 'Indicator Name', 'Indicator Code', '1960', '1961', '1962', '1963', '1964', '1965', '1966', '1967', '1968', '1969', '1970', '1971', '1972', '1973', '1974', '1975', '1976', '1977', '1978', '1979', '1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2016', '2017', '2018', '2019', '2020', 'Unnamed: 65'], axis =1)
    df_GDP = df_GDP.copy().drop(['Country Code', 'Indicator Name', 'Indicator Code', '1960', '1961', '1962', '1963', '1964', '1965', '1966', '1967', '1968', '1969', '1970', '1971', '1972', '1973', '1974', '1975', '1976', '1977', '1978', '1979', '1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2016', '2017', '2018', '2019', '2020', 'Unnamed: 65'], axis =1)


    #Getting the values that are not in the original dataset (life expectancy)
    results_population = []
    for regin in df_population["Country Name"]:
        if(not(regin in df_life_expectancy["Country"].unique())):
            results_population.append(regin)

    results_GDP = []
    for regin in df_GDP["Country Name"]:
        if(not(regin in df_life_expectancy["Country"].unique())):
            results_GDP.append(regin)

    
    ## Dropping the unwanted values from the population and GDP datasets
    for j in range (0,len(results_population)):
        df_population = df_population.copy().drop(df_population[df_population['Country Name']==results_population[j]].index,axis=0)
    
    for j in range (0,len(results_GDP)):
        df_GDP = df_GDP.copy().drop(df_GDP[df_GDP['Country Name']==results_GDP[j]].index,axis=0)

    #Getting one column for years and it's corresponding values
    df_population = pd.melt(df_population,id_vars=['Country Name'],var_name='Years',value_name='Population')
    df_GDP = pd.melt(df_GDP,id_vars=['Country Name'],var_name='Years',value_name='GDP')

    #Filling the missing values of population in the original dataset with the values from the population dataset
    df_population['Years'] = df_population['Years'].astype(int)
    for i in range(0,len(df_life_expectancy['Country'])-1):
        for j in range(0,len(df_population['Country Name'])-1):
            if(df_life_expectancy['Country'][i] == df_population['Country Name'][j]):
                if(df_life_expectancy['Year'][i] == df_population['Years'][j]):
                    if((np.isnan(df_life_expectancy['Population'][i])) | (not(np.isnan(df_population['Population'][j])))):
                        df_life_expectancy['Population'][i] = df_population['Population'][j]
    #Filling the missing values of GDP in the original dataset with the values from the GDP dataset
    df_GDP['Years'] = df_GDP['Years'].astype(int)
    for i in range(0,len(df_life_expectancy['Country'])-1):
        for j in range(0,len(df_GDP['Country Name'])-1):
            if(df_life_expectancy['Country'][i] == df_GDP['Country Name'][j]):
                if(df_life_expectancy['Year'][i] == df_GDP['Years'][j]):
                    if(np.isnan(df_life_expectancy['GDP'][i]) | (not(np.isnan(df_GDP['GDP'][j])))):
                        df_life_expectancy['GDP'][i] = df_GDP['GDP'][j]

    df_life_expectancy = df_life_expectancy.groupby('Country').mean().reset_index()
    df_life_expectancy = df_life_expectancy.copy().drop(['Year'],axis=1)
    df_life_expectancy = df_life_expectancy.rename(columns={'Life expectancy ':'Life Expectancy','infant deaths':'Infant Deaths','percentage expenditure':'Percentage of Expenditure','Measles ':'Measles',' BMI ':'BMI','under-five deaths ':'Under 5Y/O Deaths','Total expenditure':'Total Expenditure' ,'Diphtheria ':'Diphtheria',' HIV/AIDS':'HIV/AIDS',' thinness  1-19 years':'Thinness in 10-19 years',' thinness 5-9 years':'Thinness in 5-9 years'})


    df_life_expectancy['name'] = df_life_expectancy['Country']
    df_life_expectancy =  df_life_expectancy.drop(['Country'], axis=1)

   
    df_explored = df_65_Index[['Id','Change forest percentable 1900 to 2012','Female Suicide Rate 100k people','MaleSuicide Rate 100k people','Forest area percentage of total land area 2012','Homicide rate per 100k people 2008-2012','Gender Inequality Index 2014','Prison population per 100k people',
    ]]

    df_explored['Gender Inequality'] = df_explored['Gender Inequality Index 2014'] 
    df_explored['Prison population'] = df_explored['Prison population per 100k people'] 
    df_explored['Homicide %'] = df_explored['Homicide rate per 100k people 2008-2012'] 
    df_explored['MaleSuicide %'] = df_explored['MaleSuicide Rate 100k people']
    df_explored['FemaleSuicide %'] = df_explored['Female Suicide Rate 100k people']
    df_explored['Forest area %'] = df_explored['Forest area percentage of total land area 2012']
    df_explored['Change forest %'] = df_explored['Change forest percentable 1900 to 2012']
    df_explored['name'] = df_explored['Id']

    df_65_explored = df_explored.drop(['Id','Change forest percentable 1900 to 2012','Female Suicide Rate 100k people','MaleSuicide Rate 100k people','Forest area percentage of total land area 2012','Homicide rate per 100k people 2008-2012','Gender Inequality Index 2014','Prison population per 100k people',
    ],axis=1)

    ## Integration
    df_happiness_lifeExp = pd.merge(df_happiness[['name','Happiness Score','Freedom','Trust','GDP']], df_life_expectancy[['name','Adult Mortality','Infant Deaths','Under 5Y/O Deaths','Hepatitis B','HIV/AIDS','Population','Life Expectancy']], on='name')
    df_250Country_external = pd.merge(df_250Country[['name','region','subregion','Literacy Rate(%)','gini','area','Inflation(%)']], df_65_explored[['name','Change forest %','Forest area %','FemaleSuicide %','MaleSuicide %','Homicide %','Prison population','Gender Inequality']], on='name')
    df_all_merged = pd.merge(df_happiness_lifeExp,df_250Country_external, on='name')

    df_all_merged_groupBy_subregion = df_all_merged.groupby('subregion').mean().reset_index()
    df_all_merged_means = df_all_merged.mean()
    df_all_merged_groupBy_subregion = df_all_merged_groupBy_subregion.fillna(value={'Literacy Rate(%)': df_all_merged_means['Literacy Rate(%)'], 'Inflation(%)': df_all_merged_means['Inflation(%)']})
    unique_subregions = df_all_merged_groupBy_subregion['subregion'].unique()
    for subregion in unique_subregions:
        df_all_merged[df_all_merged['subregion'] == subregion] = df_all_merged[df_all_merged['subregion'] == subregion].fillna( 
        value={
                'Literacy Rate(%)':df_all_merged_groupBy_subregion[df_all_merged_groupBy_subregion['subregion'] == subregion]['Literacy Rate(%)'].values[0],
                'Inflation(%)': df_all_merged_groupBy_subregion[df_all_merged_groupBy_subregion['subregion'] == subregion]['Inflation(%)'].values[0],
                'gini': df_all_merged_groupBy_subregion[df_all_merged_groupBy_subregion['subregion'] == subregion]['gini'].values[0], 
                'Hepatitis B': df_all_merged_groupBy_subregion[df_all_merged_groupBy_subregion['subregion'] == subregion]['Hepatitis B'].values[0]})


    ## 3 Engineering Features

    df_all_merged['Suicide %'] = df_all_merged['FemaleSuicide %'] + df_all_merged['MaleSuicide %']
    area = df_all_merged['area'].values.astype(float)
    population = df_all_merged['Population'].values.astype(float)
    density = population/area
    df_all_merged['Density'] = density

    

    #Calculate adult mortality per population.
    # population = df_all_merged['Population'].values.astype(float)
    percentage = 1000/population
    adult = df_all_merged['Adult Mortality'].values.astype(float)
    df_all_merged['Adult Mortality per Population'] = adult*percentage
    return df_all_merged


def store_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='transform_data')
    df.to_csv("./data/all_merged.csv")



E = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag,
)


T= PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform_data,
    dag=dag,
)

L= PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=store_data,
    dag=dag,
)



# step 5 - define dependencies
E >> T >> L
