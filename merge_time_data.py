import pandas as pd
import json
path_LB_csv = r'C:\Taranis\05 Time data\All teams\All_Teams_LB.csv'
path_DB_csv = r'C:\Taranis\05 Time data\All teams\All_teams_DB.csv'
path_query = r'C:\Taranis\05 Time data\All teams\Query.csv'


df_LB = pd.read_csv(path_LB_csv)
df_DB = pd.read_csv(path_DB_csv)
df_query = pd.read_csv(path_query)

merged = df_LB.merge(df_DB, left_on='ImageID', right_on='imageid', how='inner',left_index=False).set_index('ImageID')


instructions = []
for i in merged['data']:
    json_data = json.loads(i)

    if json_data["need_to_tag"]=='False':
        instructions.append('ai')
    elif "tag_weeds_species" in json_data:
        if json_data["tag_weeds_species"] == 'True':
            instructions.append('Weeds Species')


    elif json_data["tag_weeds"]=='True' and json_data["tag_emergence"]=='False':
        instructions.append('Weeds(Broad/Grass)')
    elif json_data["tag_weeds"]=='False' and json_data["tag_emergence"]=='True':
        instructions.append('Emergence')
    elif json_data["tag_weeds"]=='True' and json_data["tag_emergence"]=='True':
        instructions.append('Emergence + Weeds(Broad/Grass)')

merged["Tagging_Instructions"] = instructions
merged = merged[merged.createdBy !='ai@taranis.ag']

merged_date = merged.merge(df_query, left_on='project', right_on='Key', how='left')

print(len(merged))

# path_test = r'C:\Taranis\05 Time data\MTM\merged_date.csv'
path_out = r'C:\Taranis\05 Time data\All teams\merged.csv'


merged_date.to_csv((path_out),index=False)