def pythonoperator(task_id, python_callable):

    str = task_id +" = PythonOperator( \n" \
          "    task_id='"+task_id+"', \n" \
          "    python_callable="+python_callable+",\n" \
          "    dag=dag)\n"

    return str