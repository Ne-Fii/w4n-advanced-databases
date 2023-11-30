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

    # Adding Btree index on user_id
    #* Point 3.2, 3.4
    #! Index on the primary key! Not counted to the grade!
    IDX_USERS = """
    CREATE INDEX IF NOT EXISTS idx_users
    ON users USING btree (user_id);
    """

    # Adding Btree index on user_date_of_birth
    #* Point 3.5 / Counted to the grade
    IDX_USER_BIRTH = """
    CREATE INDEX IF NOT EXISTS idx_users_birth
    ON users USING btree (user_date_of_birth);
    """

    # Adding Btree index on post_id
    #* Point 3.7
    #! Index on the primary key! Not counted to the grade!
    IDX_POSTS = """
    CREATE INDEX IF NOT EXISTS idx_posts 
    ON posts USING btree (post_id);
    """

    # Adding Btree index on comment_id
    #* Point 3.7
    #! Index onthe primary key! Not counted to the grade!
    IDX_COMMENTS = """
    CREATE INDEX IF NOT EXISTS idx_comments 
    ON comments USING btree (comment_id);
    """

    # Adding Btree index on sale_id
    #! Index onthe primary key! Not counted to the grade!
    # IDX_SALES_OF_USER = """
    # CREATE INDEX IF NOT EXISTS idx_sales_of_user
    # ON Marketplace USING btree(sale_id);
    # """

    #Adding Btree index to employee_updated
    #* Point 3.1 / Counted to the grade
    IDX_EMPLOYEE_UPDATED = """
    CREATE INDEX IF NOT EXISTS idx_employee_updated
    ON employees USING btree(employee_updated_at);
    """

    #Adding Btree index to employee_salary
    #* Point 3.1 / Counted to the grade
    IDX_EMPLOYEE_SALARY = """
    CREATE INDEX IF NOT EXISTS idx_employee_salary
    ON employees USING btree(employee_salary);
    """

    # Adding Btree index to employee_id
    #* Point 3.3 
    #! Index on the primary key
    IDX_EMPLOYEES = """
    CREATE INDEX IF NOT EXISTS idx_employees 
    ON employees USING btree (employee_id);
    """

    conn, curr = connect()
    curr.execute(IDX_USERS)
    curr.execute(IDX_POSTS)
    curr.execute(IDX_COMMENTS)
    #curr.execute(IDX_SALES_OF_USER)
    curr.execute(IDX_EMPLOYEE_SALARY)
    curr.execute(IDX_EMPLOYEE_UPDATED)
    curr.execute(IDX_USER_BIRTH)
    conn.commit()
    conn.close()


# That might be good for experimental part
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
    # IDX_POSTS_BTREE = """
    # CREATE INDEX idx_posts ON posts 
    # USING btree 
    # (
    #     regexp_split_to_table(COALESCE(string_agg(p.post_content, ' '), ''), E'\\s+')
    # );
    # """
    # IDX_COMMENTS_GIST = """
    # CREATE INDEX IF NOT EXISTS idx_comments 
    # ON comments USING gist (comment_id);
    # """

    # Adding function index using gin() to comment and post content
    #* Point 3.4 / Counted to the grade
    IDX_POST_COMMENT_CONTENT = """
    CREATE INDEX idx_posts_post_content_function 
    ON posts 
    USING gin(regexp_split_to_array(post_content, E'\\s+'));

    CREATE INDEX idx_comments_comment_content_function 
    ON comments 
    USING gin(regexp_split_to_array(comment_content, E'\\s+'));
    """



    conn, curr = connect()
    # curr.execute(IDX_POSTS_BTREE)
    # curr.execute(IDX_COMMENTS_GIST)
    curr.execute(IDX_POST_COMMENT_CONTENT)
    conn.commit()
    conn.close()

# Adding composite indexes to user_id, post_id, comment_id
#* Point 3.2 / Counted to the grade
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

# Adding bitmap index to percentile_rank
#* Point 3.3 / Counted to the grade
def create_bitmap():
    IDX_PERCENTILE_RANK = """
    CREATE INDEX idx_percentile_rank 
    ON RankedEmployees
    USING BITMAP (percentile_rank);
    """

    conn, curr = connect()
    curr.execute(IDX_PERCENTILE_RANK)
    conn.commit()
    conn.close()