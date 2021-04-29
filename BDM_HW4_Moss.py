from datetime import datetime
from datetime import timedelta
import json
import math
import pyspark
import os
import sys
def run(file_name):
    sc = pyspark.SparkContext()
    sp = pyspark.sql.SparkSession(sc)
    core_places_file = 'hdfs:///data/share/bdm/core-places-nyc.csv'
    weekly_pattern_file = 'hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*'
    output_prefix = file_name
    os.mkdir(output_prefix)
    def extract_safegraphid_naics_code(partId,records):
        if partId==0:
            next(records)
        import csv
        reader = csv.reader(records)
        valid_naics_codes = ['722511','722513','452210','452311','445120','722410','446110','446191','311811','722515', '445210', '445220', '445230', '445291', '445292','445299','445110']
        for row in reader:
            if row[9] in valid_naics_codes:
                yield (row[1],row[9])

    places = sc.textFile(core_places_file, use_unicode=False).cache()
    valid_places = places.mapPartitionsWithIndex(extract_safegraphid_naics_code)
    def extract_safegraphid_date_visits(partId,records):
        if partId==0:
            next(records)
        import csv
        reader = csv.reader(records)
        
        for row in reader:    
            yield (row[1],(row[12],row[13],row[16]))

    restaurants = sc.textFile(weekly_pattern_file, use_unicode=False).cache()
    mapped_restaurant_records = restaurants.mapPartitionsWithIndex(extract_safegraphid_date_visits)


    place_naics_dict = {'big_box_grocers':['452210','452311'],'convenience_stores':['445120'], 'drinking_places':['722410'],'full_service_restaurants':['722511'],'limited_service_restaurants':['722513'],'pharmacies_and_drug_stores':['446110','446191'],
                        'snack_and_bakeries':['311811','722515'],'specialty_food_stores':[ '445210', '445220', '445230', '445291', '445292','445299'],'supermarkets_except_convenience_stores':['445110']}
    
    def custom_std(data, ddof=1):
        n = len(data)
        mean = sum(data) / n
        v = sum((x - mean) ** 2 for x in data) / (n - ddof)
        return math.sqrt(n)

    def calc_med_hi_lo(kv):
        values = kv[1]
        year = kv[0][:4]
        sorted_values = sorted(list(values))
        mid = len(sorted_values) // 2
        median = (sorted_values[mid] + sorted_values[~mid]) / 2
        std = custom_std(sorted_values)
        low = max(0,median-std)
        hi = median + std
        return (year,kv[0],low,median,hi)

    def expandRows(partId,iterator): 
        for row in iterator:
            start_day = row[1][1][0][:10]
            visits = json.loads(row[1][1][2])
            for i in range(0,7):
                date_obj = datetime.strptime(start_day,"%Y-%m-%d") + timedelta(days=i)
                yield (date_obj.strftime("%Y-%m-%d"),visits[i])
    joined_records = valid_places.join(mapped_restaurant_records)
    for k,v in place_naics_dict.items():
        limited_service_restaurants = joined_records.filter(lambda x: x[1][0] in v)
        day_visits = limited_service_restaurants.mapPartitionsWithIndex(expandRows)
        final_values = day_visits.groupByKey().map(calc_med_hi_lo)
        
        if final_values.isEmpty():
            continue
        df = sp.createDataFrame(data=final_values).toDF("year","date", "low","median","high")
        df = df.orderBy("date")
        df.show()
        df.write.csv("{output_prefix}/{k}".format(output_prefix=output_prefix, k=k))
        

if __name__ == "__main__":
    file_name = sys.argv[1]
    run(file_name)
