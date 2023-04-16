import psycopg2

class QueryMetricsCollector:
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None

    def connect(self):
        """Connect to the PostgreSQL database."""
        self.conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cur = self.conn.cursor()

    def disconnect(self):
        """Disconnect from the PostgreSQL database."""
        self.cur.close()
        self.conn.close()
    
    def lowercase_phrase_except_within_double_quotes(self, phrase):
        """
        Converts a phrase to lowercase, but preserves the words enclosed in double quotes (" ").
        Also removes the extra spaces between the words and also remove the space between  '*' and word.

        Args:
            phrase (str): The input phrase to convert.

        Returns:
            str: The converted phrase with lowercase words except for words within double quotes.
        """
        words = phrase.split()  # split the phrase into words by spaces between words
        converted_words = []
        within_quotes = False
        for word in words:
            if word.startswith('"'):
                within_quotes = True
            if within_quotes:
                converted_words.append(word)
            else:
                if word.startswith('*'):
                    if( len(word) > 1):
                        converted_words.append('* ' + word[1:].lower())
                    else :
                        converted_words.append('*')
                elif word.endswith('*'): # if word ends with '*' then add space between word and '*'
                    converted_words.append(word[:-1].lower() + ' *')
                
                elif "*" in word:
                    # Find the index of '*'
                    index = word.find('*')

                    # Split the string into three parts
                    part1 = word[:index].lower()  # "select"
                    part2 = word[index:index + 1]  # "*"
                    part3 = word[index + 1:].lower()  # "from"

                    # Combine the parts with spaces
                    result = part1 + ' ' + part2 + ' ' + part3
                    converted_words.append(result)
                else:
                    converted_words.append(word.lower())

            if word.endswith('"'):
                within_quotes = False

        return ' '.join(converted_words)



    def get_query_metrics(self, query_text):
        """Retrieve query metrics from pg_stat_statements view for a specific query text."""
        self.connect()

        # create pg_stat_statements extension if it doesn't exist
        self.cur.execute("""create extension if not exists pg_stat_statements""")
        self.conn.commit() # commit the query to create pg_stat_statements extension
        
        # reset pg_stat_statements
        # self.cur.execute("""select pg_stat_statements_reset()""")
        # self.conn.commit() # commit the query to reset pg_stat_statements
        # first execute the query to populate pg_stat_statements
        self.cur.execute(query_text)
        self.conn.commit() # commit the query to populate pg_stat_statements

        # Execute query to retrieve query metrics for the specified query text

        # Note: The query text in pg_stat_statements is stored in lowercase, so we need to convert
        # the query text to lowercase before querying pg_stat_statements.
        # query_text = self.lowercase_phrase_except_within_double_quotes(query_text)

        self.cur.execute("""
            SELECT query , total_exec_time, total_plan_time, calls, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time, temp_blk_read_time, temp_blk_write_time, wal_records, wal_fpi, wal_bytes, jit_generation_time, jit_inlining_count, jit_inlining_time, jit_optimization_count, jit_optimization_time, jit_emission_count, jit_emission_time FROM pg_stat_statements
        """)
        query_metrics = self.cur.fetchall()
        # print(query_metrics)
        # select only the query metrics for the specified query text

        # Note: The query text in pg_stat_statements is stored in lowercase, so we need to convert
        # the query text to lowercase before querying pg_stat_statements.
        query_text = self.lowercase_phrase_except_within_double_quotes(query_text)
        # change row[0] to in specific format to match with query_text
        query_metrics = [row for row in query_metrics if self.lowercase_phrase_except_within_double_quotes(row[0]) == query_text]
        
        result = {}
        if len(query_metrics) != 0:
            result['query_text'] = query_metrics[0][0]
            result['total_exec_time'] = query_metrics[0][1]
            result['total_plan_time'] = query_metrics[0][2]
            result['calls'] = query_metrics[0][3]
            result['rows'] = query_metrics[0][4]
            result['shared_blks_hit'] = query_metrics[0][5]
            result['shared_blks_read'] = query_metrics[0][6]
            result['shared_blks_dirtied'] = query_metrics[0][7]
            result['shared_blks_written'] = query_metrics[0][8]
            result['local_blks_hit'] = query_metrics[0][9]
            result['local_blks_read'] = query_metrics[0][10]
            result['local_blks_dirtied'] = query_metrics[0][11]
            result['local_blks_written'] = query_metrics[0][12]
            result['temp_blks_read'] = query_metrics[0][13]
            result['temp_blks_written'] = query_metrics[0][14]
            result['blk_read_time'] = query_metrics[0][15]
            result['blk_write_time'] = query_metrics[0][16]
            result['temp_blk_read_time'] = query_metrics[0][17]
            result['temp_blk_write_time'] = query_metrics[0][18]
            result['wal_records'] = query_metrics[0][19]
            result['wal_fpi'] = query_metrics[0][20]
            result['wal_bytes'] = query_metrics[0][21]
            result['jit_generation_time'] = query_metrics[0][22]
            result['jit_inlining_count'] = query_metrics[0][23]
            result['jit_inlining_time'] = query_metrics[0][24]
            result['jit_optimization_count'] = query_metrics[0][25]
            result['jit_optimization_time'] = query_metrics[0][26]
            result['jit_emission_count'] = query_metrics[0][27]
            result['jit_emission_time'] = query_metrics[0][28]
            
            result['total_time'] = result['total_exec_time'] + result['total_plan_time'] + result['blk_read_time'] + result['blk_write_time'] + result['temp_blk_read_time'] + result['temp_blk_write_time'] + result['jit_generation_time'] + result['jit_inlining_time'] + result['jit_optimization_time'] + result['jit_emission_time'] 
            result['cpu_time'] = result['total_exec_time'] - result['total_plan_time']
            result['total_memory_usage'] = (result['shared_blks_hit'] + result['shared_blks_read'] + result['shared_blks_dirtied'] + result['shared_blks_written'] + result['local_blks_hit'] + result['local_blks_read'] + result['local_blks_dirtied'] + result['local_blks_written'] + result['temp_blks_read'] + result['temp_blks_written'])
            result['block_io_time'] = result['blk_read_time'] + result['blk_write_time'] + result['temp_blk_read_time'] + result['temp_blk_write_time']


        self.disconnect()
        
        query_result = []
        if ( len(result) == 0 ):
            return query_result
        query_result.append(result['query_text'])
        query_result.append(result['total_time'])
        query_result.append(result['calls'])
        query_result.append(result['rows'])
        query_result.append(result['cpu_time'])
        query_result.append(result['total_memory_usage'])
        query_result.append(result['block_io_time'])
        return query_result


# Example usage:
# Initialize the QueryMetricsCollector with PostgreSQL connection details
db_name = 'term_project'
user = 'postgres'
password = 'dakshana'
host = '127.0.0.1'
port = '5432'
query_metrics_collector = QueryMetricsCollector(db_name, user, password, host, port) 

# Execute a query and collect query metrics
query = 'SELECT * FROM "Employee"'

query_metrics = query_metrics_collector.get_query_metrics(query)


    

