import requests
import pandas as pd
import json
import psycopg2

try:
    # call api
    response = requests.get('url')

    # load json
    json_data = json.loads(response.text)

    # Normalize data
    # df = pd.json_normalize(json_data, record_path=['content'], meta=['Project', 'SYSID', 'Service', 'TLC', 'hostname'])
    df = pd.json_normalize(json_data['result'])

    # Keep required columns
    df = df[['resource', 'content.Project', 'content.SYSID', 'content.Service', 'content.TLC', 'content.hostname']]

    # rename columns
    df = df.set_axis(['resource', 'Project', 'SYSID', 'Service', 'TLC', 'hostname'], axis=1, inplace=False)

    # create csv
    # df.to_csv('out.csv',index=False)


    try:
        # establish connection
        conn = psycopg2.connect(host='',
                                port='',
                                user='',
                                password='',
                                database='', )

        cur = conn.cursor()

        #create table
        cur.execute("CREATE TABLE redifi (id serial PRIMARY KEY , resource TEXT, Project text, SYSID text, Service text, TLC text, hostname text);")

        df = df.reset_index()  # make sure indexes pair with number of rows
        for index, row in df.iterrows():
            #print(row['resource'], row['Project'], row['SYSID'], row['Service'], row['TLC'], row['hostname'])

            query = """
            INSERT into redifi(resource,Project,SYSID,Service,TLC,hostname) VALUES('%s','%s','%s','%s','%s','%s');
            """ % (row['resource'], row['Project'], row['SYSID'], row['Service'], row['TLC'], row['hostname'])
            cur.execute(query)
            conn.commit() 

        # print data for vm:mdvmsrv1444
        cur.execute("SELECT Project, SYSID, Service, TLC, hostname FROM redifi WHERE resource='vm name';")
        print("Data for vm:mdvmsrv1444:",cur.fetchone())

        # print all table data
        cur.execute("SELECT * FROM redifi")
        all_table_data = cur.fetchall()
        print("All table data:")
        for row in all_table_data:
            print(row)

        # cur.execute("DROP TABLE redifi")
        # conn.commit()

        conn.close()
        cur.close()

    except psycopg2.DatabaseError as error:
        print(error)

except requests.exceptions.RequestException as error:  # This is the correct syntax
    print(error)













