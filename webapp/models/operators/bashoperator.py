def bashoperator(dplid, bash_command):

    str = "pull_"+dplid+" = BashOperator( \n" \
          "    task_id='pull_"+dplid+"', \n" \
          "    bash_command="+bash_command+",\n" \
          "    dag=dag)\n"

    return str