function date_list = WorkingDaysList(start_date, end_date)
% 获取工作日列表


    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);

    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);

    if isdatetime(start_date) && isdatetime(end_date)
        start_date_str = datestr(start_date, 'yyyy-mm-dd');
        end_date_str = datestr(end_date, 'yyyy-mm-dd');
    elseif ischar(start_date) || isstring(start_date)
        start_date_str = char(start_date);
        if isdatetime(end_date)
            end_date_str = datestr(end_date, 'yyyy-mm-dd');
        elseif ischar(end_date) || isstring(end_date)
            end_date_str = char(end_date);
        else
            error('end_date格式无效');
        end
    else
        error('start_date格式无效');
    end

    query = ['SELECT * FROM data_prepared_new.chinesevaluationdate WHERE valuation_date BETWEEN ''', start_date_str, ''' AND ''', end_date_str, ''''];
    date_list = fetch(conn, query);
    close(conn);

    fprintf_log('工作日列表: %s 到 %s，共 %d 天\n', ...
        start_date_str, end_date_str, height(date_list));
end
