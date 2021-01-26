from labelbox import Client
import pandas as pd
import json


if __name__ == '__main__':
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJja2R5aGo3YnNiN2p2MDc2Mmd5czZhbzVpIiwib3JnYW5pemF0aW9uSWQiOiJjazRzZjZqMWdpdDV2MDkyMXU1YXd2dmhtIiwiYXBpS2V5SWQiOiJja2phMjNxNzBtYzNrMDc1OTF4YWxmYm4xIiwiaWF0IjoxNjA5MjUwMzQxLCJleHAiOjIyNDA0MDIzNDF9.LOXHYVufUZFItPu87Mm4VwC2ki1k-VlI1XLVNvo8xDU"
    client = Client(API_KEY)


# Ops = '''["OPS-63544","OPS-63240","OPS-63119","OPS-63118","OPS-63117","OPS-63115","OPS-62872","OPS-63816","OPS-63572","OPS-62870"]'''
Ops = '''["OPS-63232",
 "OPS-63238",
 "OPS-63549",
 "OPS-63546",
 "OPS-63550",
 "OPS-63234",
 "OPS-66124",
 "OPS-63242",
 "OPS-63241",
 "OPS-63114",
 "OPS-63121",
 "OPS-63120",
 "OPS-63116",
 "OPS-63390",
 "OPS-61134",
 "OPS-63097",
 "OPS-66109",
 "OPS-62871",
 "OPS-65565",
 "OPS-63544",
 "OPS-63233",
 "OPS-63237",
 "OPS-63236",
 "OPS-66108",
 "OPS-66121",
 "OPS-63240",
 "OPS-63816",
 "OPS-63817",
 "OPS-63815",
 "OPS-63818",
 "OPS-63117",
 "OPS-63118",
 "OPS-63115",
 "OPS-63119",
 "OPS-62872",
 "OPS-62870",
 "OPS-63572",
 "OPS-63563",
 "OPS-66922",
 "OPS-65562",
 "OPS-65564",
 "OPS-65563",
 "OPS-65566",
 "OPS-65567"]'''


def get_id(ops):
    id_data = client.execute('''
  {
  projects(where:{name_in:%s}){
	  id
  }
  }
''' % (ops))
    data_json = str(id_data).replace("\'", "\"")
    df_json = json.loads(data_json)
    df_id = pd.DataFrame(df_json)
    df_id['projects'] = df_id['projects'].astype(str).str.slice(len("{'id':  "), -2)
    return list(df_id['projects'])
# Ops = '''["OPS-68034","OPS-69861","OPS-69860","OPS-69862","OPS-69863"]'''



def get_data_list(id):
    data = client.execute('''
    {
      project(where:{id:"%s"}){
        labelCount
        labels(first:100){

          createdBy{
            email
          }
          project{
            name
          }
          dataRow{
            externalId
          }
          secondsToLabel
        }

      }
    }
    ''' % (id))
    data_json = str(data).replace("\'", "\"")

    json_data = json.loads(data_json)
    return json_data['project']


def get_data_list_over100(id, skip):
    data = client.execute('''
    {
      project(where:{id:"%s"}){
  labelCount
    labels(skip:%s,first:100){

        createdBy{
        email  
      }
      project{
        name

      }
      dataRow{
        externalId

      }
      secondsToLabel


      }

    }
  }
    ''' % (id, skip))
    data_json = str(data).replace("\'", "\"")

    json_data = json.loads(data_json)
    return json_data['project']


def labelCount_check(ops):
    data = client.execute('''
      {
      projects(where:{name_in:%s}){
      	    id
            labelCount
        	name
      }
      }
    ''' % (ops))
    data_json = str(data).replace("\'", "\"")
    json_data = json.loads(data_json)
    json_l = json_data['projects']
    return json_l


id_list = get_id(Ops)
data_appended = []

#                       Main loop with loop for projects with over and under 100 images :

for i in id_list:
    data_list = get_data_list(i)
    num_images = data_list['labelCount']
    if num_images > 100:
        skips = (num_images // 100)
        for j in range(skips + 1):
            temp_list = get_data_list_over100(i, j * 100)['labels']
            for row in temp_list:
                data_appended.append(row)
    else:

        for x in range(len(data_list['labels'])):
            data_appended.append(data_list['labels'][x])



print(data_appended)
print(len(data_appended))

df_combined = pd.DataFrame(data_appended)
df_combined = df_combined.rename(columns={'dataRow': 'ImageID'})  # Cleaning data

df_combined['ImageID'] = df_combined['ImageID'].astype(str).str.slice(len("{'externalId': '"), -2)
df_combined['ImageID'] = df_combined['ImageID'].astype(int)
df_combined['project'] = df_combined['project'].astype(str).str.slice(len("{'name': '"), -2)
df_combined['createdBy'] = df_combined['createdBy'].astype(str).str.slice(len("{'email': '"), -2)

path = r'C:\Taranis\05 Time data\All teams\All_Teams_LB.csv'
df_combined.to_csv(path, index=False)

