from src.database import connect
from src.utils import monitor_function


from src.task2 import bad_users, deleting_all_user_not_connected_for_one_year, get_average_age_of_users, increase_all_employee_salaries_by_10_percent_every_year, look_for_the_most_common_word, most_engaged_users, raise_salary_best_moderators, select_all_user_informations

def delete_all_indexes():
    QUERY = """
    drop index if exists idx_users;
    drop index if exists idx_posts;
    drop index if exists idx_comments;
    drop index if exists idx_sales_of_user;
    drop index if exists idx_employees;
    """
    conn, curr = connect()
    curr.execute(QUERY)
    conn.commit()
    conn.close()

def create_indexes_btree():

    IDX_USERS = """
    CREATE INDEX IF NOT EXISTS idx_users
    ON users USING btree (user_id);
    """

    IDX_POSTS = """
    CREATE INDEX IF NOT EXISTS idx_posts 
    ON posts USING btree (post_id);
    """
    IDX_COMMENTS = """
    CREATE INDEX IF NOT EXISTS idx_comments 
    ON comments USING btree (comment_id);
    """
    IDX_SALES_OF_USER = """
    CREATE INDEX IF NOT EXISTS idx_sales_of_user
    ON Marketplace USING btree(sale_id);
    """
    
    IDX_EMPLOYEES = """
    CREATE INDEX IF NOT EXISTS idx_employees 
    ON employees USING btree (employee_id);
    """

    conn, curr = connect()
    curr.execute(IDX_USERS)
    curr.execute(IDX_POSTS)
    curr.execute(IDX_COMMENTS)
    curr.execute(IDX_SALES_OF_USER)
    conn.commit()
    conn.close()


def create_indexes_hash():
    IDX_USERS = """
    CREATE INDEX IF NOT EXISTS idx_users
    ON users USING hash (user_id);
    """
    IDX_POSTS = """
    CREATE INDEX IF NOT EXISTS idx_posts 
    ON posts USING hash (post_id);
    """
    IDX_COMMENTS = """
    CREATE INDEX IF NOT EXISTS idx_comments 
    ON comments USING hash (comment_id);
    """
    
    IDX_EMPLOYEES = """
    CREATE INDEX IF NOT EXISTS idx_Employees 
    ON employees USING hash (employee_id);
    """
    
    IDX_SALES_OF_USER = """
    CREATE INDEX IF NOT EXISTS idx_sales_of_user
    ON marketplace USING hash(sale_id);
    """

    conn, curr = connect()
    curr.execute(IDX_USERS)
    curr.execute(IDX_POSTS)
    curr.execute(IDX_COMMENTS)
    curr.execute(IDX_SALES_OF_USER)
    
    conn.commit()
    conn.close()

def create_indexes_function():
    IDX_POSTS = """
    CREATE INDEX idx_posts ON posts 
    USING btree 
    (
        regexp_split_to_table(COALESCE(string_agg(p.post_content, ' '), ''), E'\\s+')
    );
    """
    IDX_COMMENTS = """
    CREATE INDEX IF NOT EXISTS idx_comments 
    ON comments USING gist (comment_id);
    """

    conn, curr = connect()
    curr.execute(IDX_POSTS)
    curr.execute(IDX_COMMENTS)
    conn.commit()
    conn.close()

def create_indexes_composite():
    IDX_COMPOSITE = """
    CREATE INDEX idx_user_post_comment ON public.users (user_id)
    INCLUDE (user_id),
    public.comments (user_id, comment_id),
    public.posts (user_id, post_id);
    """

    conn, curr = connect()
    curr.execute(IDX_COMPOSITE)
    conn.commit()
    conn.close()

    


def task5():
    logs = None
    # clean_logs()

    # ? no bitmap indexes in postgresql ?
    index_names = ["btree", "hash"]

    delete_all_indexes()
    
    for _ in range(2):
            monitor_function(select_all_user_informations, index=False)()
            monitor_function(raise_salary_best_moderators, index=False)()
            monitor_function(look_for_the_most_common_word, index=False)()
            #monitor_function(most_engaged_users, index=FALSE)()
            #monitor_function(get_average_age_of_users, index=FALSE)()
            #monitor_function(increase_all_employee_salaries_by_10_percent_every_year, index=True, index_name=index_name)()
            #monitor_function(deleting_all_user_not_connected_for_one_year, index=True, index_name=index_name)()
            #monitor_function(bad_users, index=False)()

    for index_name in index_names:
        delete_all_indexes()
        
        if index_name == "btree":
            create_indexes_btree()
        elif index_name == "hash":
            create_indexes_hash()
        for _ in range(2):
            monitor_function(select_all_user_informations, index=True, index_name=index_name)()
            monitor_function(raise_salary_best_moderators, index=True, index_name=index_name)()
            monitor_function(look_for_the_most_common_word, index=True, index_name=index_name)()
            # monitor_function(most_engaged_users, index=True, index_name=index_name)()
            # monitor_function(get_average_age_of_users, index=True, index_name=index_name)()
            # monitor_function(increase_all_employee_salaries_by_10_percent_every_year, index=True, index_name=index_name)()
            # monitor_function(deleting_all_user_not_connected_for_one_year, index=True, index_name=index_name)()
            #monitor_function(bad_users, index=True, index_name=index_name)()
