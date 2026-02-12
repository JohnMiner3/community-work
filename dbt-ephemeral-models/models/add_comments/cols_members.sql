{{ config(materialized='ephemeral') }}

--
--  Add descriptions to columns within members01 table
--


  {# 1. define key/val pairs for column comments #}

  {% set data_list = [
    {'col': 'MemberID', 'desc': 'The primary key for the members table.'},
    {'col': 'Name', 'desc': 'The name of the member.'},
    {'col': 'Email', 'desc': 'The email address of the member.'},
    {'col': 'JoinDate', 'desc': 'The date the member joined the library.'},
    {'col': 'Status', 'desc': 'The current status of the member.'},
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
            @level1name = N'members01',
            @level2type = N'COLUMN', 
            @level2name = N'{{ item.col }}';
      {% endset %}

      {% do exec_tsql_cmd(query) %}

    {% endfor %}
  {% endif %}
