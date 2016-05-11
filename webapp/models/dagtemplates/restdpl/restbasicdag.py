from ...operators.pythonoperator import pythonoperator

def createdag(pipelinename, scheduleinterval):
    # Create a file in the DAG folder of Airflow
    fo = open("./airflow/dags/"+pipelinename+".py", "wb")
    # Write imports
    fo.write("from airflow.operators import PythonOperator \n")
    fo.write("from airflow.models import DAG \n")
    fo.write("from datetime import datetime \n")
    fo.write("from datetime import datetime \n")
    fo.write("from producers.restdpl.restbasicdpl import pull \n")
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

    fo.write("#Call Python Operator \n")
    fo.write(pythonoperator(task_id='pull_'+pipelinename, python_callable='pull("'+pipelinename+'")'))
    # Close open file
    fo.close()
    return