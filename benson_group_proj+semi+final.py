


low = avg_daily_mean.traffic.quantile(.7)
high = avg_daily_mean.traffic.quantile(.9)
daily_mean_for_merge = avg_daily_mean[(avg_daily_mean.traffic >= low) & (avg_daily_mean.traffic <= high)]


# In[268]:

# Get unique list of stations
stations_df = final_mta.groupby(['station','linename']).head(1)
stations_df = stations_df.assign(join_station_line = stations_df.station + ' ' + stations_df.linename)
stations = stations_df['join_station_line']

# In[286]:

# Read in income data
inc = pd.read_csv('income_by_subway_stops.csv')
inc.columns = [n.lower().replace(' ', '_') for n in inc.columns]


# In[287]:

# Clean income data
inc = inc.dropna(subset=['stop_name']) # We don't have data for the Shuttle trains. We drop the nulls.


# In[288]:

# Make a new column combining 'stop_name' with every 'subway_line' for that stop
inc.sort_values(['lat', 'long', 'subway_line'], inplace=True)
inc2 = inc.groupby(['lat','long']).head(1)
line_names = inc.groupby(['lat','long']).subway_line.agg(lambda x: ''.join(x)).tolist()
inc2 = inc2.assign(line_names = line_names)
inc2.head(20)


# In[289]:

# Run .upper on stop_name and combine with line_names
inc2 = inc2.assign(station_line = inc2.stop_name.str.upper() + ' ' + inc2.line_names)

# Check for multiple entries
print inc2.groupby('station_line').size().sort_values(ascending=False)[:10]  # We don't have any! ^_^
print inc2.shape


# In[291]:

# Find the station_line values in the 'stations' list that most closely match what is in inc2
inc2.loc[:,'join_station_line'] = inc2.station_line.apply(lambda x:                                         difflib.get_close_matches(x, stations, n=1, cutoff=.3))
inc2.join_station_line = inc2.join_station_line.apply(lambda x: x[0])


# In[292]:

# Check for duplicate join names
print inc2.sort_values('join_station_line').groupby('join_station_line').filter(lambda x: len(x) >1).loc[
    :,['income2011','station_line','join_station_line']]

# We have some duplicates and errors. Fix the ones that need fixing:
inc2.loc[441, 'join_station_line'] = '14 ST 123FLM'
inc2.loc[272, 'join_station_line'] = '125 ST ACBD'
inc2.loc[135, 'join_station_line'] = '3 AV 138 ST 6'
inc2.loc[365, 'join_station_line'] = 'NEW UTRECHT AV ND'
inc2.loc[224, 'join_station_line'] = '5 AVE 7BDFM'
inc2.loc[188, 'join_station_line'] = 'BLEECKER ST 6DF'
inc2.loc[615, 'join_station_line'] = 'BEVERLEY ROAD BQ'
inc2.loc[264, 'join_station_line'] = 'FULTON ST ACJZ2345'

print '\n', inc2.sort_values('join_station_line').groupby('join_station_line').filter(lambda x: len(x) >1).loc[
    :,['income2011','station_line','join_station_line']]
# This looks better. Next we'll drop the duplicates, taking the entry with higher income.


# In[293]:

# Drop dupes
inc2 = inc2.sort_values(['join_station_line', 'income2011'], ascending=False).groupby('join_station_line').head(1)
inc2.shape


# In[324]:

# Drop columns for merge
inc3 = inc2[['lat', 'long', 'county_name', 'income2011', 'join_station_line']]
inc3.head(20)


# In[325]:

# Merge income and lat/long data into our subway traffic data
daily_mean_merge = pd.merge(daily_mean_for_merge, inc3, how='left', on='join_station_line')


# In[326]:

# Check result
print daily_mean_merge.shape
print daily_mean_merge.sort_values('income2011', na_position='first')     [['traffic', 'join_station_line', 'lat', 'income2011']][:20] # We have some nulls!


# In[327]:

# Manually fix a few join names
inc3 = inc3.append(inc3.loc[132])
inc3.iloc[423, 4] = '59 ST NQR456'

inc3 = inc3.append(inc3.loc[13])
inc3.iloc[424, 4] = 'TIMES SQ-42 ST ACENQRS1237'
inc3 = inc3.append(inc3.loc[13])
inc3.iloc[425, 4] = '42 ST-PORT AUTH ACENGRS1237'

inc3 = inc3.append(inc3.loc[268])
inc3.iloc[426, 4] = '8 AV ACEL'

inc3 = inc3.append(inc3.loc[49])
inc3.iloc[427, 4] = 'ATL AV-BARCLAY BDNQR2345'


# In[348]:

# Join again and check results
daily_mean_merge = pd.merge(daily_mean_for_merge, inc3, how='left', on='join_station_line')

print daily_mean_merge.shape
print daily_mean_merge.sort_values('income2011', na_position='first')     [['traffic', 'join_station_line', 'lat', 'income2011']][:20] 
    
# Looks good. The remaining null are all the PATH trains. We'll drop those.
inc3.dropna(inplace=True)


# In[349]:

daily_mean_merge.rename(columns={'income2011':'income'}, inplace=True)
daily_mean_merge.shape


# In[350]:

daily_mean_merge.income.hist()


# In[351]:

daily_mean_merge.sort_values('income', ascending=False)


# In[352]:

# Drop high-traffic stations that weren’t caught in the aggregate drop (since
# some stations were split into multiple)
daily_mean_merge.drop([59,39,65,58,53,0,19,20], inplace=True)

# Combine stations that were split into multiple that we want to keep
daily_mean_merge.loc[9, 'traffic'] += daily_mean_merge.loc[15, 'traffic']
daily_mean_merge.loc[5, 'traffic'] += daily_mean_merge.loc[17, 'traffic']
daily_mean_merge.drop([15, 17], inplace = True)


# In[353]:

daily_mean_merge.sort_values('income', ascending=False).head(50)


# In[364]:

# Concatenate long/lat and export to .csv
daily_mean_merge['longlat'] = daily_mean_merge['long'].astype(str) + ', ' + daily_mean_merge['lat'].astype(str)
daily_mean_merge.sort_values('income', ascending=False).head(50).to_csv('final_stations.csv')



