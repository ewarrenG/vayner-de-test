import pandas as pd
import numpy as np
import time
#q2
from datetime import datetime
from dateutil.parser import parse
#q3
import json

#higher order functions
def read_and_format_source1():
	#read source1_csv
	df1 = pd.read_csv("source1.csv")
	#separate audience col
	i = df1.columns.get_loc('audience')
	df1_2 = df1['audience'].str.split("_", expand=True)
	df1_2.columns = ['state', 'hair_color', 'age']
	df1_3 = pd.concat([df1.iloc[:, :i], df1_2, df1.iloc[:, i+1:]], axis=1)
	return df1_3.drop_duplicates()

def read_and_format_source2():
	df2 = pd.read_csv("source2.csv")
	df2_1 = df2.join(df2['actions'].apply(json.loads).apply(pd.Series))
	return df2_1

#globals
source1_raw = read_and_format_source1()
source2_raw = read_and_format_source2()

#q1: what was the total spent against people with purple hair?
def question1(data, color):
	source1 = data["one"]
	#get ids of all campaign_ids with hair color as list
	purple_ids = source1.loc[source1['hair_color'] == color, 'campaign_id']
	source2 = data["two"]
	#get all campaign_ids from source 2 that have purple_ids
	source2_purples = source2[source2['campaign_id'].isin(purple_ids)]
	return source2_purples["spend"].values.sum()

#q2: how many campaigns spent on more than 4 days?
def question2(data, days):
	source2 = data["two"]
	#group rows by campaign_id, then date
	grouped_source2 = source2.groupby(["campaign_id"])["date"]
	count_greater_than = 0
	#iterate through and get length of groupings
	for name, group in grouped_source2:
		if len(group) > days:
			count_greater_than += 1
	return count_greater_than

#q3: how many times did source H report on clicks?
def question3(data, source):
	#filter source2 for rows that contain source
	source2 = data["two"].loc[data["two"]["actions"].str.contains(source), :]
	source_click_count = 0
	#iterate through rows
	for index, row in source2.iterrows():
		i = 0
		#iterate through converted actions columns
		while i < 5: 
			try:
				#make sure cell is dict, has source H and action is clicks
				if type(row[i]) is dict and row[i][source] and row[i]["action"] == "clicks":
					source_click_count += row[i][source]
			except KeyError:
				pass
			i += 1
	return source_click_count

#q4: which sources reported more "junk" than "noise"?
def question4(data, actions_str):
	#filter source2 for rows that contain junk or noise
	source2 = data["two"].loc[data["two"]["actions"].str.contains(actions_str),:]
	#create empty dicts
	noise_obj = {}
	junk_obj = {}
	#iterate through rows
	for index, row in source2.iterrows():
		#iterate through converted actions columns
		i = 0
		while i < 5: 
			try:
				if type(row[i]) is dict:
					for key, value in row[i].items():
						#check action noise
						if row[i]["action"]    == "noise":
							#increment value
							if key in noise_obj:
								noise_obj[key] += value
							#assign value
							else:
								noise_obj[key] = value
						if row[i]["action"]    == "junk":
							if key in junk_obj:
								junk_obj[key] += value
							else:
								junk_obj[key] = value
			except KeyError:
				pass
			i += 1

	return_arr = []
	for key, value in noise_obj.items():
		if junk_obj[key] > noise_obj[key]:
			return_arr.append(key)
	return return_arr

#q5: what was the total cost per view for all video ads, truncated to two decimal places?
def question5(data, ad_type, action_str):
	#filter source2 for rows with ad_type video
	source2 = data["two"].loc[data["two"]["ad_type"] == ad_type, :]
	view_sum = 0
	video_spend_sum = 0
	#iterate through rows
	for index, row in source2.iterrows():
		#sum video spend
		video_spend_sum += row["spend"]
		#iterate through converted action columns
		i = 0
		while i < 5: 
			try:
				if type(row[i]) is dict:
					for key, value in row[i].items():
						#make sure action is action_str
						if row[i]["action"] == action_str:
						    view_sum += value
						    break
			except KeyError:
				pass
			i += 1
	#compute and return total cost per view of all video ads
	return round(video_spend_sum/view_sum, 2)

#q6: how many source B conversions were there for campaigns targeting NY?
def question6(data, source, action_str, state):
	#filter source1 and create list of ids with state param
	state_ids = data["one"].loc[data["one"]["state"] == state, "campaign_id"].tolist()
	#filter source2 for rows that contain conversions
	source2 = data["two"].loc[data["two"]["actions"].str.contains(action_str), :]
	source_state_conversion = {source: 0}
	#iterate through rows
	for index, row in source2.iterrows():
		#check row is in state_ids
		if row["campaign_id"] in state_ids:
			#iterate through converted actions columns
			i = 0
			while i < 5: 
				try:
					if type(row[i]) is dict:
						for key, value in row[i].items():
							#make sure conversion and desired source
							if row[i]["action"] == action_str and key in source_state_conversion:
								source_state_conversion[key] += value
				except KeyError:
					pass
				i += 1
	#return source conversions for state
	return source_state_conversion[source]
		

#7. what combination of state and hair color had the best CPM?
#most impressions, lowest cost
def question7(data):
	source1 = data["one"]
	#iterate and create two dictionaires
	#state_hair_impressions key state-hair combo, value sum of impressions
	#state_hair_ids key state-hair combo, value array of ids
	state_hair_impressions = {}
	state_hair_ids = {}
	for index, row in source1.iterrows():
		if(row["state"] + "-" + row["hair_color"]) in state_hair_impressions:
			state_hair_impressions[row["state"] + "-" + row["hair_color"]] += row["impressions"]
			if row["campaign_id"] not in state_hair_ids[row["state"] + "-" + row["hair_color"]]:
				state_hair_ids[row["state"] + "-" + row["hair_color"]].append(row["campaign_id"])
		else:
			state_hair_impressions[row["state"] + "-" + row["hair_color"]] = row["impressions"]    
			state_hair_ids[row["state"] + "-" + row["hair_color"]] = [row["campaign_id"]]

	source2 = data["two"]
	#iterate and create dictionary
	#campaign_id_spend key is id, value sum of spend
	campaign_id_spend = {}
	for index, row in source2.iterrows():
		if row["campaign_id"] in campaign_id_spend:
			campaign_id_spend[row["campaign_id"]] += row["spend"]
		else:
			campaign_id_spend[row["campaign_id"]] = row["spend"]			
	#print("len(campaign_id_spend)")
	#print(len(campaign_id_spend))

	#Analysis
	#iterate and create dictionary key is state-hair combo, value is compute cpm
	state_hair_cpm = {}
	number_of_ids_in_state_hair_ids = 0
	#itereate through state_hair_ids and state_hair_impressions simultaneously
	for (k, v), (k2, v2) in zip(state_hair_ids.items(), state_hair_impressions.items()):
		number_of_ids_in_state_hair_ids += len(v)
		#iterate through state_hair_ids value (array of ids) and get spend amt from campaign_id_spend
		#compute cpm as spend / impressions * 1000
		#add to state_hair_cpm
		for i in v:
			if i in campaign_id_spend:
				state_hair_cpm[k] = round((campaign_id_spend[i] / v2) * 1000, 2)
	
	#print("number_of_ids_in_state_hair_ids") 
	#print(number_of_ids_in_state_hair_ids) #why doesn't this value equal the length of campaign_id_spend??
	#return key of minimum value of state_hair_cpm
	return min(state_hair_cpm, key=state_hair_cpm.get).split("-")




#function calls and runtimes
start_time = time.time()
q1_start_time = time.time()
print('q1:', question1({"one": source1_raw, "two": source2_raw}, "purple"))
q1_end_time = time.time()
#print("--- q1 runtime %s seconds ---" % (q1_end_time - q1_start_time)) 
q2_start_time = time.time()
print('q2:', question2({"two": source2_raw}, 4)) 
q2_end_time = time.time()
#print("--- q2 runtime %s seconds ---" % (q2_end_time - q2_start_time))
q3_start_time = time.time()
print('q3:', question3({"two": source2_raw}, "H")) 
q3_end_time = time.time()
#print("--- q3 runtime %s seconds ---" % (q3_end_time - q3_start_time))
q4_start_time = time.time()
print('q4:', question4({"two": source2_raw},"junk|noise")) 
q4_end_time = time.time()
#print("--- q4 runtime %s seconds ---" % (q4_end_time - q4_start_time))
q5_start_time = time.time()
print('q5:', question5({"two": source2_raw}, "video", "views")) 
q5_end_time = time.time()
#print("--- q5 runtime %s seconds ---" % (q5_end_time - q5_start_time))
q6_start_time = time.time()
print('q6:', question6({"one": source1_raw, "two": source2_raw}, "B", "conversions", "NY")) 
q6_end_time = time.time()
#print("--- q6 runtime %s seconds ---" % (q6_end_time - q6_start_time))"""
q7_start_time = time.time()
print('q7:', question7({"one": source1_raw, "two": source2_raw})) 
q7_end_time = time.time()
#print("--- q7 runtime %s seconds ---" % (q7_end_time - q7_start_time))
print("--- runtime %s seconds ---" % (time.time() - start_time))
