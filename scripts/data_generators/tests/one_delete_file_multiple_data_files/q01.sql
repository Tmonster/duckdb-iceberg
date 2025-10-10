UPDATE default.one_delete_file_multiple_data_files
Set l_comment=NULL,
    l_quantity=NULL,
    l_discount=NULL,
    l_linestatus=NULL
where l_linenumber = 3 or l_linenumber = 4 or l_linenumber = 5;