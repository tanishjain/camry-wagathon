import streamlit as st
import sqlite3
import time
import uuid
import pandas as pd
import re

st.set_page_config(layout="wide", page_title='CLP Scrum Pointing Poker', initial_sidebar_state="collapsed")

def hide_hamburger() :
    hide_streamlit_style = """
                    <style>
                    div[data-testid="stToolbar"] {
                    visibility: hidden;
                    height: 0%;
                    position: fixed;
                    }
                    div[data-testid="stDecoration"] {
                    visibility: hidden;
                    height: 0%;
                    position: fixed;
                    }
                    div[data-testid="stStatusWidget"] {
                    visibility: hidden;
                    height: 0%;
                    position: fixed;
                    }
                    #MainMenu {
                    visibility: hidden;
                    height: 0%;
                    }
                    header {
                    visibility: hidden;
                    height: 0%;
                    }
                    footer {
                    visibility: hidden;
                    height: 0%;
                    }
                    </style>
                    """

    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

hide_hamburger()


####################  CONFIG  #######################

db = 'pointing_poker.sqlite'
voting_events_table = 'voting_events'
voting_statuses_table = 'voting_statuses'
users_table = 'users'
users_columns = ['name', 'timestamp']
users_datatypes = ['text', 'text']
voting_events_columns = ['voting_id', 'name', 'vote', 'timestamp']
voting_events_datatypes = ['text', 'text', 'text', 'text']
voting_statuses_columns = ['voting_id', 'voting_status', 'timestamp']
voting_statuses_datatypes = ['text', 'int', 'text']
allowed_votes = ['?', '☕', '0', '0.5', '1', '2', '3', '5', '8', '13', '21']
user_retention_time = '-2 hours'
regex = re.compile('[^a-zA-Z]')



##################### INIT SESSION STATE ###########################
if 'user_name' not in st.session_state :
    st.session_state['user_name'] = ''

if 'team_name' not in st.session_state :
    st.session_state['team_name'] = ''

if 'host_user' not in st.session_state :
    st.session_state['host_user'] = False

if 'voting_id' not in st.session_state :
    st.session_state['voting_id'] = ''

    
@st.cache_resource
def sqlite_connection() :
    conn_sqlite = sqlite3.connect(db, check_same_thread=False)
    cur_sqlite = conn_sqlite.cursor()
    return[conn_sqlite, cur_sqlite]

conn_sqlite, cur_sqlite = sqlite_connection()

def reset_database() :
    voting_events_ddl_cols = ','.join([f'{col} {dtype}' for col, dtype in zip(voting_events_columns, voting_events_datatypes)])
    voting_statuses_ddl_cols = ','.join([f'{col} {dtype}' for col, dtype in zip(voting_statuses_columns, voting_statuses_datatypes)])
    users_ddl_cols = ','.join([f'{col} {dtype}' for col, dtype in zip(users_columns, users_datatypes)])
    
    cur_sqlite.execute(f"drop table if exists {st.session_state['team_name']}__{voting_events_table}")
    conn_sqlite.commit()
    cur_sqlite.execute(f"create table {st.session_state['team_name']}__{voting_events_table} ({voting_events_ddl_cols})")
    conn_sqlite.commit()
    
    cur_sqlite.execute(f"drop table if exists {st.session_state['team_name']}__{voting_statuses_table}")
    conn_sqlite.commit()
    cur_sqlite.execute(f"create table {st.session_state['team_name']}__{voting_statuses_table} ({voting_statuses_ddl_cols})")
    conn_sqlite.commit()
    
    cur_sqlite.execute(f"""drop view if exists {st.session_state['team_name']}__{voting_statuses_table}_last""")
    conn_sqlite.commit()
    
    cur_sqlite.execute(f"""create view {st.session_state['team_name']}__{voting_statuses_table}_last as
                       with cte as (select {', '.join(voting_statuses_columns)} , row_number() over (partition by voting_id order by timestamp desc) as RN
                       from {st.session_state['team_name']}__{voting_statuses_table}
                       )
                       select * from cte where RN = 1""")
    conn_sqlite.commit()
    
    cur_sqlite.execute(f"""drop view if exists {st.session_state['team_name']}__{voting_events_table}_last""")
    conn_sqlite.commit()
    
    cur_sqlite.execute(f"""create view {st.session_state['team_name']}__{voting_events_table}_last as
                       with cte as (select {', '.join(voting_events_columns)} , row_number() over (partition by voting_id, name order by timestamp desc) as RN
                       from {st.session_state['team_name']}__{voting_events_table}
                       )
                       select * from cte where RN = 1""")
    conn_sqlite.commit()
    
    cur_sqlite.execute(f"drop table if exists {st.session_state['team_name']}__{users_table}")
    conn_sqlite.commit()
    cur_sqlite.execute(f"create table {st.session_state['team_name']}__{users_table} ({users_ddl_cols})")
    conn_sqlite.commit()
    
def insert_user(team_name, user_name) :
    try :
        cur_sqlite.execute(f"""insert into {team_name}__{users_table}
                           ({', '.join(users_columns)} )
                           VALUES ('{user_name}' , datetime('now') ) """)
        conn_sqlite.commit()
    except :
        reset_database()
        cur_sqlite.execute(f"""insert into {team_name}__{users_table}
                           ({', '.join(users_columns)} )
                           VALUES ('{user_name}' , datetime('now') ) """)
        conn_sqlite.commit()

    
query_params = st.experimental_get_query_params()
if 'host_user' in query_params :
    if query_params['host_user'] :
        st.header('You are the host :sunglasses:')
        st.session_state['host_user'] = True
        
if 'team_name' in query_params :
    if query_params['team_name'] :
        st.session_state['team_name'] = regex.sub('', str(query_params['team_name'])).lower()
        


if st.session_state['user_name'] == '' or st.session_state['team_name'] == '' :
    with st.form("Login as: ") :
        user_name  = st.text_input('Login name: ', '', key="user_name_input" )
        team_name = st.text_input('Team: ', st.session_state['team_name'])
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.session_state['user_name'] = user_name
            st.session_state['team_name'] = regex.sub('', team_name).lower()
            insert_user(st.session_state['team_name'], st.session_state['user_name'])
   


def get_voting_id() :
    try :
        voting_id = pd.read_sql_query(f"""select max(voting_id) as voting_id from {st.session_state['team_name']}__{voting_statuses_table}_last
                                  where timestamp = (select max(timestamp) from {st.session_state['team_name']}__{voting_statuses_table}_last
                                  where voting_status > -1)""", conn_sqlite )['voting_id'].to_list()[0]
        st.session_state['voting_id'] = voting_id or ''
        if not voting_id :
            st.write("Waiting for host to start the voting")
    except :
        st.write("Waiting for host to start the voting")

if st.session_state['team_name'] != '' and st.session_state['user_name'] != '' :
    get_voting_id()

st.write("Logged in as: ", st.session_state['user_name'])


def show_results(voting_id) :
    df = pd.read_sql_query(f"""with unioned as (SELECT
	                          e.name,
	                          case 
                                  when s.voting_status = 1 and e.vote = "⌛" then e.vote
                                  when s.voting_status = 1 and e.name <> '{st.session_state['user_name']}' then "❓"
                                  else e.vote
                              end as vote,
                              e.timestamp
                          from {st.session_state['team_name']}__{voting_events_table}_last e
                          inner join {st.session_state['team_name']}__{voting_statuses_table}_last s
                          on s.voting_id = e.voting_id
                          where e.voting_id = '{voting_id}'
                          
                          UNION ALL
                          
                          select name, "⌛" as vote, timestamp
					      from {st.session_state['team_name']}__{users_table}
                          where timestamp > datetime('now', '{user_retention_time}')
                          ),
                          ranked as (select name, vote, row_number() over (partition by name order by timestamp desc) as RN from unioned)
                          
                          select name, vote from ranked where RN = 1
                          
                          """, conn_sqlite)
    return df
    
    
def is_revealed(voting_id) :
    rev = pd.read_sql_query(f"""select voting_status from {st.session_state['team_name']}__{voting_statuses_table}_last where voting_id = '{voting_id}' """, conn_sqlite)['voting_status'].to_list()
    if len(rev) > 0 :
        rev = rev[0]
    else :
        return False
    if rev == 1 :
        return False
    else :
        return True

def voted(voting_id, user_name) :
    if len(pd.read_sql_query(f"""select name from {st.session_state['team_name']}__{voting_events_table} where voting_id = '{voting_id}' """, conn_sqlite)['name'].to_list()) > 0 :
        return True
    return False

#####################  ADMIN PANEL ############################
if (st.session_state['host_user'] == True) and (len(st.session_state['user_name']) > 1) and (len(st.session_state['team_name']) > 1):
    #with st.expander(f"Reset/init database (Team name: {st.session_state['team_name']})") :
    #    reset_db = st.checkbox('Reset database', key="reset_db_checkbox")    
    #    if reset_db :
    #        confirm = st.button('Are you sure?', key="confirm_reset_database")
    #        if confirm :
    #            reset_database()
    #            st.toast('Database reset')
    #            time.sleep(3)
                    
    
    reveal_notes = st.button("Reveal votes", key="reveal_votes")
    if reveal_notes :
        try :
            cur_sqlite.execute(f"""insert into {st.session_state['team_name']}__{voting_statuses_table}
                               ({', '.join(voting_statuses_columns)} )
                               VALUES ('{st.session_state['voting_id']}', '0', datetime('now') )""")
            conn_sqlite.commit()
        except:
            reset_database()
            cur_sqlite.execute(f"""insert into {st.session_state['team_name']}__{voting_statuses_table}
                               ({', '.join(voting_statuses_columns)} )
                               VALUES ('{st.session_state['voting_id']}', '0', datetime('now') )""")
            conn_sqlite.commit()
                    
    
    next_voting = st.button("Start/Next voting", key="next_voting")
    if next_voting :
        st.session_state['voting_id'] = uuid.uuid4().hex
        try :
            cur_sqlite.execute(f"""insert into {st.session_state['team_name']}__{voting_statuses_table}
                               ({', '.join(voting_statuses_columns)} )
                               VALUES ('{st.session_state['voting_id']}', '1', datetime('now') )""")
            conn_sqlite.commit()
        except :
            reset_database()
            cur_sqlite.execute(f"""insert into {st.session_state['team_name']}__{voting_statuses_table}
                               ({', '.join(voting_statuses_columns)} )
                               VALUES ('{st.session_state['voting_id']}', '1', datetime('now') )""")
            conn_sqlite.commit()


            

if st.session_state['voting_id'] != '' :
    with st.form("Place your vote!") :
        if voted(st.session_state['voting_id'], st.session_state['user_name']) == False :
            cur_sqlite.execute(f"""insert into {st.session_state['team_name']}__{voting_events_table}
            ({', '.join(voting_events_columns)})
            VALUES ('{st.session_state['voting_id']}', '{st.session_state['user_name']}', '⌛', datetime('now'))""")
            conn_sqlite.commit()
        
        vote = st.radio('Your vote: ', allowed_votes, horizontal = True)
        
        submitted = st.form_submit_button("Submit")
        if submitted:
            if is_revealed(st.session_state['voting_id']) == False :
                st.write("Your vote: ", vote)
                cur_sqlite.execute(f"""insert into {st.session_state['team_name']}__{voting_events_table}
                ({', '.join(voting_events_columns)})
                VALUES ('{st.session_state['voting_id']}', '{st.session_state['user_name']}', '{vote}', datetime('now'))""")
                conn_sqlite.commit()
            else :
                st.error("""The voting has ended.""")
    
    placeholder = st.empty()    
    
    while True :
        get_voting_id()
        df_res = show_results(st.session_state['voting_id'])
        if is_revealed(st.session_state['voting_id']) :
            df_to_avg = df_res.copy()
            df_to_avg['vote'] = pd.to_numeric(df_to_avg['vote'], errors="coerce")
            average_vote = round(df_to_avg['vote'].mean(), 1)
            df_avg_append = pd.DataFrame([{'name' : 'Average points:', 'vote' : str(average_vote)}])
            df_res = pd.concat([df_res, df_avg_append], ignore_index = True)
        
        placeholder.dataframe(df_res, hide_index=True, height=((len(df_res) + 1) * 42) )
    
        time.sleep(2)
