def bashoperator(taskid, bash_command):

    str = taskid+" = BashOperator( \n" \
          "    task_id='"+taskid+"', \n" \
          "    bash_command="+bash_command+",\n" \
          "    dag=dag)\n"

    return str