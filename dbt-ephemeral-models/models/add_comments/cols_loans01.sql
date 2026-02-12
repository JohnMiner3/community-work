{{ config(materialized='ephemeral') }}

--
--  Add descriptions to columns within loans01 table
--


  {# 1. define key/val pairs for column comments #}

  {% set data_list = [
    {'col': 'LoanID', 'desc': 'The primary key for the loans table.'},
    {'col': 'BookID', 'desc': 'The ID of the book on loan.'},
    {'col': 'MemberID', 'desc': 'The ID of the member who borrowed the book.'},
    {'col': 'BorrowDate', 'desc': 'The date the book was borrowed.'},
    {'col': 'DueDate', 'desc': 'The date the book is due to be returned.'},
    {'col': 'ReturnDate', 'desc': 'The date the book was returned.'},
  ] %}

      
  {# 2. apply column comments #}

  {% if execute %}
    {% for item in data_list %}

      {% set query %}
        EXEC sp_addextendedproperty 
            @name = N'MS_Description', 
            @value = N'{{ item.desc }}', 
            @level0type = N'SCHEMA', 
            @level0name = N'data_raw', 
            @level1type = N'TABLE', 
            @level1name = N'loans01',
            @level2type = N'COLUMN', 
            @level2name = N'{{ item.col }}';
      {% endset %}

      {% do exec_tsql_cmd(query) %}

    {% endfor %}
  {% endif %}
