from webapp.models.operators.pythonoperator import pythonoperator

def createdag(pipelinename, url, scheduleinterval, headers, payload):
    # Create a file in the DAG folder of Airflow
    fo = open("../airflow/dags/"+pipelinename+".py", "wb")
    # Write imports
    fo.write("import requests \n")
    fo.write("import json \n")
    fo.write("import pandas \n")
    fo.write("from airflow.operators import PythonOperator \n")
    fo.write("from airflow.models import DAG \n")
    fo.write("from datetime import datetime \n")
    fo.write("\n")
    # Write the default arguments of the DAG
    default_args = "{'owner': 'damstream','start_date': datetime(2016, 4, 21)}"
    fo.write("default_args = "+default_args)
    fo.write("\n")
    #DAG Configuration
    fo.write("\n")
    fo.write("dag = DAG(\n"
             "    '"+pipelinename+"', default_args=default_args, \n"
             "    schedule_interval='@"+scheduleinterval+"')")
    fo.write("\n")
    #RESTful Data Consumption
    fo.write("\n")
    url = "'"+url+"'"
    headers = headers
    payload = payload
    output_dir = "'/home/damstream/"+pipelinename+".csv'"
    fo.write("def get_data(): \n")
    fo.write("    payload = "+payload+"\n")
    fo.write("    URL = "+url+"\n")
    fo.write("    headers = "+headers+"\n")
    fo.write("    r = requests.post(URL, headers=headers, data=json.dumps(payload))\n")
    fo.write("    js = json.loads(r.content)\n")
    fo.write("    df = pandas.DataFrame.from_dict(js)\n")
    fo.write("    df.to_csv("+output_dir+", index=False)\n")

    fo.write("\n")
    fo.write("#Call Python Operator \n")
    fo.write(pythonoperator(task_id='get_data', python_callable='get_data'))
    # Close open file
    fo.close()
    return