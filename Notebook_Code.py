# From first chunk, producing hr_output_indicators

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark2 = SparkSession.builder.getOrCreate()

df_azure = spark.sql("SELECT * FROM Lakehouse_HR_PROD.rawmyGCHR")

# Clean Column Names by cleaning and standardizing the column names of a DataFrame (df_azure) by removing or replacing certain characters.
columns = df_azure.columns
def clean_column_name(column_name):
    return column_name.strip().replace(" ", "_").replace(";", "").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace("=", "")
for col in columns:
    df_azure = df_azure.withColumnRenamed(col, clean_column_name(col))

###############################
#                             #
# HR Cleaning Transformations #
#                             #
###############################

# Cleaning code creates indicators (Sunset, Substantive_Old, Substantive, Secondment) based on various conditions. Finally, it adjusts the date format for the "Eff_Date" column.
# converting data type to a date format.
df_azure = df_azure.withColumn("Eff_Date", F.to_date(F.col("Eff_Date"), "M/d/yyyy"))

refSunsetIndicator = df_azure.filter(F.col("Reason_Descr") == "Sunset Program").groupBy("PSSI_Position", "ID").agg(F.min("Eff_Date").alias("SunsetDate"))

# Create Sunset Indicator
refSunsetIndicator = refSunsetIndicator.withColumn("SunsetDate", F.to_date(F.col("SunsetDate"), "M/d/yyyy"))

# Join Sunset Indicators
df_azure_joined = df_azure.join(refSunsetIndicator, ['PSSI_Position', 'ID'], 'left_outer')
df_azure_joined = df_azure_joined.withColumn("Sunset", F.when((F.col("Eff_Date").isNotNull()) & (F.col("SunsetDate").isNotNull()) & (F.col("Eff_Date") >= F.col("SunsetDate")), "T").otherwise("F"))
df_azure_joined = df_azure_joined.drop("SunsetDate")

# Add 'Substantive_Old' indicator
df_azure_joined = df_azure_joined.withColumn("Substantive_Old",F.when((F.col("Asgn/Appt_Type") == "Substantive"),1).otherwise(0))

# Add Substantive Indicator
df_azure_joined = df_azure_joined.withColumn("Substantive",F.when(F.col("Asgn/Appt_Type") == "Substantive",F.when(F.col("PSSI_Position") == "Y",2).otherwise(1)).otherwise(0))

# Add 'Secondment' indicator
df_azure_joined = df_azure_joined.withColumn("Secondment",F.when((F.col("Asgn/Appt_Type") == "Secondment In"),1).otherwise(0))

# Change Date Format
df_azure = df_azure_joined.withColumn("Eff_Date", F.to_date(F.col("Eff_Date"), "M/d/yyyy"))

##############################
#                            #
# SPARK Date Transformations #
#                            #
##############################

# hard coded start and end dates that encompass the time period that PSSI is place
start_date = datetime(2021, 1, 1).date()
end_date = datetime(2027, 12, 31).date()
num_days = (end_date - start_date).days

# Generate a DataFrame of calendar dates from the start_date to end_date
date_df_azure = spark2.range(num_days + 1).select(F.date_add(F.lit(start_date), F.col("id").cast("int")).alias("Calendar_Date"))

# Window specification to find the next `Eff Date` Window Specifications: Two window specifications are defined: One to sort and partition data by "ID" and "Empl_Record", ordered by "Eff_Date".
# Another to handle multiple entries for the same effective date, ensuring that only the entry with the maximum sequence is kept.
windowSpec_azure = Window.partitionBy("ID", "Empl_Record").orderBy("Eff_Date")

# Resolve the multiple Eff Date entries with Window to keep max Sequence
windowSpec2_azure = Window.partitionBy("ID", "Empl_Record", "Eff_date").orderBy(F.col("Sequence").desc())

# The DataFrame is processed to remove any duplicate entries based on the effective date, keeping only the one with the highest sequence value.
df_azure = df_azure.withColumn("row_num", F.row_number().over(windowSpec2_azure)).filter(F.col("row_num") == 1).drop("row_num")

# Find the next `Eff Date` for each `ID` and `Empl Record`
df_azure = df_azure.withColumn("Next_Eff_Date", F.lead("Eff_Date").over(windowSpec_azure))

# Cross join with the date DataFrame to duplicate rows. This operation duplicates rows for each date between the effective date and the next effective date (or the last available date if no next effective date exists).
df_azure = df_azure.crossJoin(date_df_azure).filter((F.col("Calendar_Date") >= F.col("Eff_Date")) & ((F.col("Calendar_Date") < F.col("Next_Eff_Date")) | (F.col("Next_Eff_Date").isNull())))

# Create Substantive Indicator (T/F)
windowSpecSubatantive_azure = Window.partitionBy("ID","Calendar_Date")
df_azure = df_azure.withColumn("Substantive_Indicator", F.max("Substantive").over(windowSpecSubatantive_azure))

# Create Substantive_Old Indicator (T/F)
windowSpecSubatantive_azure = Window.partitionBy("ID", "PSSI_Position", "Calendar_Date")
df_azure = df_azure.withColumn("Substantive_Old_Indicator", F.max("Substantive_Old").over(windowSpecSubatantive_azure))

# Create Secondment Indicator (T/F)
windowSpecSubatantive_azure = Window.partitionBy("ID", "Calendar_Date")
df_azure = df_azure.withColumn("Secondment_Indicator", F.max("Secondment").over(windowSpecSubatantive_azure))

# Create Categories
empl_class_conditions = ["Greater than 6 months", "=or> 3 months =or< 6 months", "Less than 3 months"]
acting_class_conditions = ["Acting Appointment < 4 months", "Acting Appointment =or> 4 mths"]

# retired (not used anymore) defining the employment class of each employee
df_azure = df_azure.withColumn("Empl_Class_Category_Row",
    F.when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "T"), "Sunset Term")
    .when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "F"), "Term")
    .when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Old_Indicator") == 1), "Indeterminate")
    .when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Old_Indicator") != 1), "Acting/Assignment")
    .otherwise("Other")
)

# similar to the above conditions, with the inclusion of the use of the substantive_indicator and the secondment_indicator
df_azure = df_azure.withColumn(
    "Empl_Class_Category_Substantive_OLD",
    # Sub in pssi vs not in pssi
    # if not the you are always acting
    # else rules \/ 
    F.when((F.col("Substantive_Indicator") == 1) & (F.col("PSSI_Position") == "Y"), "Acting/Assignment/Secondment")
    .when((F.col("Secondment_Indicator") == 1) & (F.col("Substantive_Indicator") != 2), "Acting/Assignment/Secondment")
    .when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "T"), "Sunset Term")
    .when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "F"), "Term")
    .when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Indicator") == 2), "Indeterminate")
    .when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Indicator") != 2), "Acting/Assignment/Secondment")
    .otherwise("Other")
)

# similar to the above conditions, but with the addition of a flag that also flags an indeterminate that is acting/assignment/secondment
# Sub in pssi vs not in pssi
    # if not the you are always acting
    # else rules \/ 
df_azure = df_azure.withColumn("Empl_Class_Category_Current_Stat",
    F.when((F.col("Substantive_Indicator") == 1) & (F.col("PSSI_Position") == "Y"), "Acting/Assignment/Secondment")
    .when((F.col("Secondment_Indicator") == 1) & (F.col("Substantive_Indicator") != 2), "Acting/Assignment/Secondment")
    .when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "T"), "Sunset Term")
    .when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "F"), "Term")
    .when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Indicator") == 2) & (F.col("Asgn/Appt_Type") == "Substantive"), "Indeterminate")
    .when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Indicator") == 2) & (F.col("Asgn/Appt_Type") != "Substantive"), "Acting/Assignment/Secondment")
    .when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Indicator") != 2), "Acting/Assignment/Secondment")
    .otherwise("Other")
)

# New column for the number of true substantives
    # Substantive position in pssi and its tenure status (Indetermiante, Sunset Term, Term)
    # Even if not currently in pssi, they should still be tracked, as long as substantive is in pssi
        # Don't want to look at acting/assignment/secondment
df_azure = df_azure.withColumn("Empl_Class_Category_PSSI_Substantive",
    F.when(
        (F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("PSSI_Position")== "Y") &
        (F.col("Sunset") == "T"), "Sunset Term")
    .when(
        (F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("PSSI_Position")== "Y") &
        (F.col("Sunset") == "F") & (F.col("Substantive_Indicator") == 2), "Term")
    .when(
        (F.col("Empl_Class_Desc") == "Indeterminate") & 
        (F.col("Substantive_Indicator") == 2) & 
        (F.col("Asgn/Appt_Type") == "Substantive"), "Indeterminate")
    .when(
        (F.col("Empl_Class_Desc") == "Indeterminate") & 
        (F.col("Substantive_Indicator") == 2) & 
        (F.col("Asgn/Appt_Type") != "Substantive"), "Indeterminate") #indetermiante is in pssi, but also acting in pssi, so still considered to be "indeterminate" for the purposes of this indicator
    .otherwise("Acting/assignment in PSSI, not PSSI Substantive")
)

# Add in end date, and duration
#df_azure = df_azure.withColumn("End_date", F.when())

# indicator to classify sunset term, term, and indeterminates
df_azure = df_azure.withColumn("Empl_Class_Category_Non_Substantive",F.when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "T"), "Sunset Term").when((F.col("Empl_Class_Desc").isin(empl_class_conditions)) & (F.col("Sunset") == "F"), "Term").when((F.col("Empl_Class_Desc") == "Indeterminate") & (F.col("Substantive_Indicator") == 1), "Indeterminate").otherwise("Other"))

af_azure = df_azure.withColumn("Position", df_azure["Position"].cast("string"))
##changed the data type from integer to string in order to keep it consistent with the position fields in other data

#test colum
#df_azure1 = df_azure.withColumn("new_column", df_azure["Position"])

# Latest Pull Date Indicator
df_azure = df_azure.withColumn("LatestPullIndicator", F.when((F.col("DataUpdateDate") == F.col("Calendar_Date")), "T").otherwise("F"))

# Added CurrentPSSIActive Indicator
df_azure = df_azure.withColumn("CurrentPSSIActive", F.when((F.col("PSSI_Position") == "Y") & (F.col("Pay_Status") == "Active") & (F.col("LatestPullIndicator") == "T"), "T").otherwise("F"))


# Output to Table
#df_azure.write.mode("overwrite").format("delta").saveAsTable("HR_ouput_Azure")

df_azure.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("HR_output_indicators")

# Print Count of Rows
# df_azure.count()


# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.

# Second chunk, producting HR_output_latesttransactions

##############################
#                            #
#  code for getting latest   #
#  transcations table and    #
#       regular terms        #
#   start and end dates      #
#                            #
##############################

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# get raw my gchr report from Azure
spark2 = SparkSession.builder.getOrCreate()
gchr_raw = spark2.sql("SELECT * FROM Lakehouse_HR_PROD.rawmyGCHR") 

# remove invalid characters from column names
columns = gchr_raw.columns
def clean_column_name(column_name):
    return column_name.strip().replace(" ", "_").replace(";", "").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace("=", "")
for col in columns:
    gchr_raw = gchr_raw.withColumnRenamed(col, clean_column_name(col))

# format 'Eff_Date' column
gchr_raw = gchr_raw.withColumn("Eff_Date", F.to_date(F.col("Eff_Date"), "M/d/yyyy"))
#check if data has been updated
#gchr_raw.show(1)


# filter raw gchr report by data update date
data_update_date = gchr_raw.select('DataUpdateDate').limit(1).collect()[0][0]
gchr = gchr_raw.filter(F.col('Eff_Date') <= data_update_date)

# find latest transactions for each ID/Empl_Record based on Eff_date and Sequence before eff date cutoff
windowSpec = Window.partitionBy("ID", "Empl_Record").orderBy(F.col("Eff_Date").desc(), F.col("Sequence").desc())
ranked_df = gchr.withColumn("rank", F.row_number().over(windowSpec))
latest_transactions = ranked_df.filter(F.col("rank") == 1).drop("rank")

# filter latest transactions related to PSSI positions
latest_transactions_pssi = latest_transactions.filter(F.col('PSSI_Position') == 'Y')
latest_transactions_pssi.count()


# assign sunset flag to IDs who've had a sunset transaction for a PSSI positin
df_sunset = gchr.filter((F.col("Reason_Descr") == "Sunset Program") & (F.col("PSSI_Position") == "Y")).select("ID").distinct()
df_sunset = df_sunset.withColumn("sunset", F.lit("y"))

# assign substantive flag to IDs whose latest position on their substantive record is a PSSI position
df_substantive = latest_transactions_pssi.filter(F.col("Asgn/Appt_Type") == "Substantive").select("ID").distinct()
df_substantive = df_substantive.withColumn("pssi_sub", F.lit("y"))

# merge sunset and substantive flags to latest_transactions_pssi
df_lt = latest_transactions_pssi.join(df_substantive, on=["ID"], how="left").\
    join(df_sunset,on=["ID"], how="left")


# assgin 'PSSI substantive empl_catetory'
# if ID is PSSI substantive: category is sunset term, term, inderterminate
# or other (specific class is listed) depending on their 'Empl_Class_Desc'
# if ID is not PSSI substantive: category is 'acting/assignment'
df_lt = df_lt.withColumn(
    "Empl_Class_Category_PSSI_Substantive",
    F.when((F.col("pssi_sub") == "y") 
            & (F.col("Empl_Class_Desc").isin(["Greater than 6 months", "=or> 3 months =or< 6 months", "Less than 3 months"]) 
                & (F.col("sunset") == "y")), 
                "Sunset Term")
                .when((F.col("pssi_sub") == "y") 
            & (F.col("Empl_Class_Desc").isin(["Greater than 6 months", "=or> 3 months =or< 6 months", "Less than 3 months"])),
            "Term")
            .when((F.col("pssi_sub") == "y") & (F.col("Empl_Class_Desc") == "Indeterminate"),
                    "Indeterminate")
                    #.when((F.col("pssi_sub") == "y"),
                        #F.col("Empl_Class_Desc"))
                        .otherwise("Acting/assignment in PSSI, not PSSI Substantive")
    )

# find substantive terms who are Active or LOA
# based on latest substantive record transaction
df_terms = df_lt.filter((df_lt["Asgn/Appt_Type"] == "Substantive") 
                        & (df_lt['Empl_Class_Category_PSSI_Substantive'].isin('Term','Sunset Term')) 
                        & (df_lt['Pay_Status'].isin(['Active', 'Leave of Absence', 'Leave With Pay'])))
df_terms_cat = df_terms.select(df_terms["ID"], df_terms["Empl_Class_Category_PSSI_Substantive"]
.alias("empl_category"))

# filter raw gchr dataframe to keep transactions for the substantive terms
gchr_terms = gchr_raw.join(df_terms_cat, on=['ID'], how="inner")

# find min date and max date
# for the substantive terms' term employment transactions
min_term_dates = gchr_terms\
    .filter(F.col("Empl_Class_Desc").isin(["Greater than 6 months", "=or> 3 months =or< 6 months", "Less than 3 months"]))\
        .groupBy('ID').agg(F.min(F.col('Eff_Date')).alias('min_term_date'))
max_term_dates = gchr_terms\
    .filter(F.col("Empl_Class_Desc").isin(["Greater than 6 months", "=or> 3 months =or< 6 months", "Less than 3 months"]))\
    .filter(gchr_terms['Reason_Descr']=='End of Specified Term')\
    .groupBy('ID').agg(F.max(F.col('Eff_Date')).alias('max_term_date'))

# get first sunset transaction date for any position (currently cannot find sunset term->regular term flag in gchr data)
first_sunset_transactions_dates = gchr_terms\
    .filter(gchr_terms['Reason_Descr']=='Sunset Program')\
    .groupBy('ID').agg(F.min(F.col('Eff_Date')).alias('first_sunset_transaction_date'))

gchr_terms = gchr_terms.join(min_term_dates, on='ID', how='left')\
    .join(max_term_dates, on='ID', how='left')\
    .join(first_sunset_transactions_dates, on='ID', how='left')
gchr_terms = gchr_terms.orderBy(F.col('Eff_Date'), F.col('Empl_Record'),F.col('Sequence'))


# function to find latest continuous term start date
def find_latest_continuous_term(rows):

    id = rows[0]
    transactions = list(rows[1])

    start_date = transactions[0].min_term_date
    sunset_date = transactions[0].first_sunset_transaction_date
    end_date = transactions[0].max_term_date
    service_break_date = None

    for t in transactions:

        # if ID had a service break date due to 1 of the reasons below
        # if new term start date (empl_class is term and pay status is active)
        # is more than 60 days later than service break date, consider this the new start date
        # otherwise, service is considered continuous, we do not change the start date
        if service_break_date is not None \
        and t['Pay_Status'] == 'Active' \
        and t['Empl_Class_Desc'] in ["Greater than 6 months", "=or> 3 months =or< 6 months", "Less than 3 months"]:
            if (t['Eff_Date'] - service_break_date).days > 60:
                
                # adjust start date to be after break in service
                start_date = t['Eff_Date']
            service_break_date = None
        
        # if previous term ended, consider this a service break date
        if t['Reason_Descr'] == 'End of Specified Term' and t['Eff_Date'] != end_date:
            service_break_date = t['Eff_Date']

        # if empl_class changes from term to something else (casual, etc)
        # after min term date, consider this a service break date
        if t['Empl_Class_Desc'] not in ["Greater than 6 months", "=or> 3 months =or< 6 months", "Less than 3 months"]\
        and t['Eff_Date'] > start_date and service_break_date is None:
            service_break_date = t['Eff_Date']
        
    return Row(ID=id, start_date = start_date, end_date = end_date, sunset_date = sunset_date)

# for each ID, apply above function to find latest_continuous_term_dates
rdd = gchr_terms.rdd.groupBy(lambda row: row["ID"])
rdd1 = rdd.map(find_latest_continuous_term)
# make new 'term_dates' dataframe
term_dates = rdd1.toDF(['ID', 'start_date', 'end_date', 'sunset_date'])


# attach latest continuous term start/end dates and sunset transaction dates
# to the latest transactions df

# months_difference = difference between start and end dates of the last continuous term identified
# accumulated_months_up_to_Feb26: for regular terms who had no sunset transaction,this is the difference between latest term start date and Feb26
# for anyone who had a sunset transaction, this is their time accumulated during the last continuous term
# (before the sunset transaction puts a hold on the accumulation)
df_lt = df_lt.join(term_dates, on='ID', how='left')
df_lt = df_lt.withColumn("months_difference", F.months_between(F.col("end_date"),\
                                                                F.col("start_date")))

df_lt = df_lt.withColumn("accumulated_months_up_to_Feb26",
                         F.when(
                             F.col("sunset_date").isNull(), 
                             F.months_between(F.lit('2024-02-26'), F.col("start_date"))
                             ).otherwise(
                                 F.months_between(F.least(F.col("sunset_date"),F.to_date(F.lit('2024-02-26'))), F.col("start_date")))
)


# read in hr_tracker and clean column names
hr_tracker = spark.read.format("csv").option("header","true").load("Files/hr_tracker0130.csv")
for col in hr_tracker.columns:
    hr_tracker = hr_tracker.withColumnRenamed(col, clean_column_name(col))

# merge hr_tracker df with latest transactions df
# so the PSSI positions that don't appear in latest transactions
# show up as a single row with blank values for the other fields
df_lt = df_lt.withColumn('Position', F.col('Position').cast("string"))
df_lt1 = df_lt.join(hr_tracker, df_lt["Position"] == hr_tracker["myGCHR_PN"], how="right")
#df_lt1.count()

#test output
# df_lt.filter(F.col('ID').isin([332616, 334103, 334187, 329422, 333897])).\
# select('ID','empl_record','start_date','end_date', 'accumulated_term_months').show()

# select columns to keep in df_lt
keep_cols = df_lt.columns + ['pn_id']
df_lt1 = df_lt1.select(*keep_cols)

# create position_staffed and position_created calculated columns
position_staffed = df_lt.filter(F.col('Pay_Status').isin('Active', 'Leave of Absence', 'Leave With Pay')).select('Position').distinct()
position_staffed = position_staffed.withColumn("position_staffed", F.lit("staffed"))
position_created = hr_tracker.filter(F.col('myGCHR_PN') !='').select('myGCHR_PN').distinct()
position_created = position_created.withColumn("position_created", F.lit("created"))
df_lt1 = df_lt1.join(position_staffed, on='Position', how='left')
df_lt1 = df_lt1.withColumn('position_staffed', F.coalesce(F.col('position_staffed'), F.lit('vacant')))
hr_tracker = hr_tracker.join(position_created, on='myGCHR_PN', how='left')
hr_tracker = hr_tracker.withColumn('position_created', F.coalesce(F.col('position_created'), F.lit('planned')))

# save the dataframes
df_lt1.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("HR_output_latestTransactions")
hr_tracker.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("HR_tracker")


# read in and save position table
position_table = spark.read.format("csv").option("header","true").load("Files/DFO_PSSI_POSITIONS-2024-06-03.csv")
for col in position_table.columns:
    position_table = position_table.withColumnRenamed(col, clean_column_name(col))
position_table = position_table.withColumn('Position', F.col('Position').substr(3,100))
position_table = position_table.withColumn('Location_Full_Address', F.concat_ws(", ", F.col('Location_Address'), F.col('City'), F.lit('Canada')))
position_table = position_table.withColumn("Classification", F.regexp_replace("Class_Code", "[^a-zA-Z]", ""))
position_table1 = position_table.join(hr_tracker, position_table["Position"] == hr_tracker["myGCHR_PN"], how="right")
keep_cols = position_table.columns + ['pn_id']
position_table1 = position_table1.select(*keep_cols)
position_table1.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("PSSI_positions")