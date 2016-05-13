from ...operators.bashoperator import bashoperator


def createdag(dplid, scheduleinterval):
    # Create a file in the DAG folder of Airflow
    fo = open("./airflow/dags/" + dplid + ".py", "wb")
    # Write imports
    fo.write("from airflow.operators import BashOperator \n")
    fo.write("from airflow.models import DAG \n")
    fo.write("from datetime import datetime \n")
    fo.write("\n")
    # Write the default arguments of the DAG
    default_args = "{'owner': 'damstream','start_date': datetime(2016, 5, 10)}"
    fo.write("default_args = " + default_args)
    fo.write("\n")
    # DAG Configuration
    fo.write("\n")
    fo.write("dag = DAG(\n"
             "    '" + dplid + "', default_args=default_args, \n"
                                      "    schedule_interval='@" + scheduleinterval + "')")
    fo.write("\n")

    fo.write("#Call Bash Operator \n")

    fo.write(bashoperator(dplid= dplid,
                          bash_command='"python -c \\"from producers.restdpl.restbasicdpl import pull; pull(\''+dplid+'\')\\""'))

    # Close open file
    fo.close()
    return
