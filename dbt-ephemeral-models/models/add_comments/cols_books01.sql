--
--  Add descriptions to columns within books01 table
--

{{ config(materialized='ephemeral') }}

  {# 1. define key/val pairs for column comments #}

  {% set data_list = [
    {'col': 'BookId', 'desc': 'The primary key for the books table.'},
    {'col': 'Title', 'desc': 'The title of the book.'},
    {'col': 'Author', 'desc': 'The author of the book.'},
    {'col': 'ISBN', 'desc': 'The International Standard Book Number of the book.'},
    {'col': 'Genre', 'desc': 'The genre of the book.'},
    {'col': 'Quantity', 'desc': 'The quantity of the book available.'},
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
            @level1name = N'books01',
            @level2type = N'COLUMN', 
            @level2name = N'{{ item.col }}';
      {% endset %}

      {% do exec_tsql_cmd(query) %}

    {% endfor %}
  {% endif %}
