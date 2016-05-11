def pythonoperator(task_id, python_callable, op_kwargs):

    str = task_id +" = PythonOperator( \n" \
          "    task_id='"+task_id+"', \n" \
          "    python_callable="+python_callable+",\n" \
          "    op_kwargs = "+op_kwargs+", \n" \
          "    dag=dag)\n"

    return str