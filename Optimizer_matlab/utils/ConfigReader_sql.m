function [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReader_sql()


    currentDir = fileparts(mfilename('fullpath'));
    db_config = fullfile(currentDir, '..', '..', 'config', 'db.yaml');
    db_info = ReadYaml(db_config);
    
    host = db_info.host3;
    dbname = db_info.database4;
    username = db_info.user2;
    password = db_info.password;
    
    % 建立数据库连接
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);
    
    if ~isempty(conn.Message)
        error('数据库连接失败: %s', conn.Message);
    end
    
    % 读取数据
    portfolio_info = read_portfolio_info(conn);
  
    portfolio_constraint = read_portfolio_constraint(conn, portfolio_info);
 
    factor_constraint = read_factor_constraint(conn, portfolio_info);

    
    % 关闭连接
    close(conn);

end

function portfolio_info = read_portfolio_info(conn)
    currentDir = fileparts(mfilename('fullpath'));
    db_config = fullfile(currentDir, '..', '..', 'config', 'db.yaml');
    db_info = ReadYaml(db_config);
    table = db_info.table_name5;

    try
        % 获取最新 session_id
        latest_session_q = sprintf('SELECT session_id FROM %s ORDER BY update_time DESC LIMIT 1', table);
        latest_session = fetch(conn, latest_session_q);


        % 提取 session 值（支持 table/cell/array 返回）
        if istable(latest_session)
            session_val = latest_session{1,1};
        elseif iscell(latest_session)
            session_val = latest_session{1,1};
        else
            session_val = latest_session(1,1);
        end


        if isnumeric(session_val)
            sel_query = sprintf('SELECT * FROM %s WHERE session_id = %s', table, num2str(session_val));
        else
            % 转为字符串并对单引号进行转义
            session_str = strrep(char(string(session_val)), '''', '''''');
            sel_query = sprintf('SELECT * FROM %s WHERE session_id = ''%s''', table, session_str);
        end

        data_cell = fetch(conn, sel_query);

        if isempty(data_cell)
            warning('没有找到指定session_id的行');
            portfolio_info = table();
            return;
        end

        % 如果fetch返回table则直接使用，否则构造table并设置列名
        if istable(data_cell)
            portfolio_info = data_cell;
        else
            colnames_query = sprintf(['SELECT column_name FROM information_schema.columns ' ...
                             'WHERE table_schema = DATABASE() AND table_name = ''%s'' ' ...
                             'ORDER BY ordinal_position'], table);
            colnames_cell = fetch(conn, colnames_query);

            if iscell(colnames_cell) && ~isempty(colnames_cell)
                colnames = colnames_cell(:, 1);
            else
                colnames = arrayfun(@(x) sprintf('col%d', x), 1:size(data_cell, 2), 'UniformOutput', false);
            end

            portfolio_info = cell2table(data_cell, 'VariableNames', colnames);
        end



        % 保持原有列顺序约定
        if ismember('portfolio_name', portfolio_info.Properties.VariableNames)
            col_order = {'portfolio_name'};
            other_cols = setdiff(portfolio_info.Properties.VariableNames, {'portfolio_name'}, 'stable');
            portfolio_info = portfolio_info(:, [col_order, other_cols]);
        end

        % 转换日期格式
        date_columns = {'start_date', 'end_date'};
        for i = 1:length(date_columns)
            col = date_columns{i};
            if ismember(col, portfolio_info.Properties.VariableNames)
                portfolio_info.(col) = datetime(portfolio_info.(col), 'InputFormat', 'yyyy-MM-dd');
            end
        end

      
        str_columns = {'portfolio_name', 'score_type', 'index_type', 'mode_type', 'user_name', 'session_id','id'};
        for i = 1:length(str_columns)
            col = str_columns{i};
            if ismember(col, portfolio_info.Properties.VariableNames)
                portfolio_info.(col) = string(portfolio_info.(col));
            end
        end

        
        try
            portfolio_info.portfolio_name = strcat(string(portfolio_info.portfolio_name), "_", string(portfolio_info.id));
        catch
            % 兼容性回退：逐行拼接
            for k = 1:height(portfolio_info)
                try
                    pn = string(portfolio_info.portfolio_name(k));
                catch
                    pn = string(char(portfolio_info.portfolio_name(k)));
                end
                portfolio_info.portfolio_name(k) = strcat(pn, "_", string(portfolio_info.id(k)));
            end
        end
        

    catch ME
       
        if isfield(ME, 'identifier') && ~isempty(ME.identifier)
            warning(ME.identifier, '%s', ME.message);
        else
            warning('ConfigReader:ReadPortfolioInfo', '%s', ME.message);
        end
        portfolio_info = table();
    end
end



function portfolio_constraint = read_portfolio_constraint(conn, portfolio_info)

    
    currentDir = fileparts(mfilename('fullpath'));
    db_config = fullfile(currentDir, '..', '..', 'config', 'db.yaml');
    db_info = ReadYaml(db_config);
    table = db_info.table_name6;

    T = fetch(conn, ['SELECT * FROM ' table]);

    if isempty(T)
        portfolio_constraint = table();
        return;
    end

    if ~istable(T)
        T = cell2table(T);
    end

   
    try
        if exist('portfolio_info', 'var') && ~isempty(portfolio_info) && ismember('session_id', T.Properties.VariableNames) && ismember('session_id', portfolio_info.Properties.VariableNames)
            try
                pi_users = string(portfolio_info.session_id);
                tc_users = string(T.session_id);
                keep_mask = ismember(tc_users, pi_users);
                T = T(keep_mask, :);
            catch
                
            end
        end
    catch
        % 忽略任何错误，继续处理原始 T
    end
  
    if ~ismember('constraint_name', T.Properties.VariableNames)
        warning('portfolio_constraint表中缺少constraint_name列，返回空表');
        portfolio_constraint = table();
        return;
    end
    
    % 获取约束列
    ignore_cols = {'constraint_name', 'update_time'};
    all_vars = T.Properties.VariableNames;
    
    constraints = {};
    for k = 1:length(all_vars)
        if ~ismember(all_vars{k}, ignore_cols)
            constraints{end+1} = all_vars{k};
        end
    end
    constraints = constraints(:);  % 确保是列向量
    
    % 检查是否有约束列
    if isempty(constraints)
        warning('portfolio_constraint表中除了忽略列外没有其他约束列，返回空表');
        portfolio_constraint = table();
        return;
    end
    
    % 创建结果表（首列为约束名）
    constraints_vec = constraints(:);
    result = cell2table(constraints_vec, 'VariableNames', {'constraint_name'});

    % 为每行数据添加一列，列名由 constraint_name 和 T 中的 id 组成以避免重名
    for i = 1:height(T)
        % 安全获取 constraint_name
        constraint_name_val = T{i, 'constraint_name'};
        if iscell(constraint_name_val)
            constraint_name_val = constraint_name_val{1};
        end

        % 尝试读取 id 列（如果存在），否则使用行号作为后备
        if ismember('id', T.Properties.VariableNames)
            id_val = T{i, 'id'};
            if iscell(id_val)
                id_val = id_val{1};
            end
        else
            id_val = i;
        end

       
        raw_name = sprintf('%s_%s', char(string(constraint_name_val)), char(string(id_val)));
        portfolio = matlab.lang.makeValidName(raw_name);

        % 若生成的变量名已存在，则追加后缀保证唯一
        if ismember(portfolio, result.Properties.VariableNames)
            k = 1;
            newname = portfolio;
            while ismember(newname, result.Properties.VariableNames)
                newname = sprintf('%s_dup%d', portfolio, k);
                k = k + 1;
            end
            portfolio = newname;
        end

        % 填充列数据
        col_data = cell(length(constraints), 1);
        for j = 1:length(constraints)
            val = T{i, constraints{j}};
            if iscell(val)
                val = val{1};
            end
            if isnumeric(val)
                col_data{j} = num2str(val);
            else
                col_data{j} = char(val);
            end
        end

        result.(portfolio) = col_data;
    end
    
    
    portfolio_constraint = result;
end



function factor_constraint = read_factor_constraint(conn, portfolio_info)

    
    currentDir = fileparts(mfilename('fullpath'));
    db_config = fullfile(currentDir, '..', '..', 'config', 'db.yaml');
    db_info = ReadYaml(db_config);
    table = db_info.table_name7;
    
    col_query = ['SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ''' table ''' ORDER BY ORDINAL_POSITION'];
    col_data = fetch(conn, col_query);
    
    column_names = cell(size(col_data, 1), 1);
    for i = 1:size(col_data, 1)
        col = col_data{i, 1};
        if iscell(col)
            column_names{i} = col{1};
        else
            column_names{i} = col;
        end
    end
    
    % 2. 读取数据
    query = 'SELECT * FROM factor_constraint';
    data = fetch(conn, query);

    if isempty(data)
        factor_constraint = table();
        return;
    end

    % 检查维度
    if size(data, 2) ~= length(column_names)
        fprintf('警告: 列数不匹配 数据有 %d 列，但列名有 %d 个\n', size(data, 2), length(column_names));

        % 使用较小的那个
        num_cols = min(size(data, 2), length(column_names));
        column_names = column_names(1:num_cols);

        % 无论 data 是否为 table，都截取前 num_cols 列以匹配列名长度
        data = data(:, 1:num_cols);
    end

    % 3. 转换为table
    if istable(data)
        T = data;
        T.Properties.VariableNames = column_names(1:width(T));
    else
        T = cell2table(data, 'VariableNames', column_names(1:size(data, 2)));
    end

    try
        if exist('portfolio_info', 'var') && ~isempty(portfolio_info) && ismember('session_id', T.Properties.VariableNames) && ismember('session_id', portfolio_info.Properties.VariableNames)
            try
                pi_users = string(portfolio_info.session_id);
                tc_users = string(T.session_id);
                keep_mask = ismember(tc_users, pi_users);
                T = T(keep_mask, :);
            catch
                disp('筛选 factor_constraint 时出错，跳过筛选步骤');
            end
        end
    catch
        
    end
    
  
    id_vals = [];
    if ismember('id', T.Properties.VariableNames)
        id_vals = cell(height(T), 1);
        for ii = 1:height(T)
            v = T{ii, 'id'};
            if iscell(v)
                v = v{1};
            end
            id_vals{ii} = string(v);
        end
    end

    % 移除不需要的列
    remove_cols = {'id', 'create_time', 'update_time'};
    for i = 1:length(remove_cols)
        col = remove_cols{i};
        if ismember(col, T.Properties.VariableNames)
            T.(col) = [];
        end
    end
    
    % 5. 检查必要的列是否存在
    if ~ismember('factor_name', T.Properties.VariableNames)
        warning('factor_constraint表中缺少factor_name列，返回空表');
        factor_constraint = table();
        return;
    end
    
    % 获取属性
    all_cols = T.Properties.VariableNames;
    % 手动过滤，确保结果是cell数组
    attributes = {};
    for k = 1:length(all_cols)
        if ~ismember(all_cols{k}, {'factor_name'})
            attributes{end+1} = all_cols{k};
        end
    end
    attributes = attributes(:);  % 确保是列向量
    
    % 检查是否有属性列
    if isempty(attributes)
        warning('factor_constraint表中除了factor_name外没有其他属性列，返回空表');
        factor_constraint = table();
        return;
    end
    
    % 6. 创建结果表
    attributes_vec = attributes(:);
    result = cell2table(attributes_vec, 'VariableNames', {'factor_name'});
    
    for i = 1:height(T)
        % 安全获取factor_name值
        factor_val = T{i, 'factor_name'};
        if iscell(factor_val)
            factor_val = factor_val{1};
        end
        fstr = char(string(factor_val));


        if ~isempty(id_vals)
            idstr = char(id_vals{i});
        else
            idstr = num2str(i);
        end

       
        tokens = regexp(fstr, '^(.*)(_lower|_upper)$','tokens','once');
        if ~isempty(tokens)
            base = tokens{1};
            suffix = tokens{2};
            raw_name = sprintf('%s_%s%s', base, idstr, suffix);
        else
            raw_name = sprintf('%s_%s', fstr, idstr);
        end

        % 转换为合法变量名并保证唯一
        factor_varname = matlab.lang.makeValidName(raw_name);
        if ismember(factor_varname, result.Properties.VariableNames)
            k = 1;
            newname = factor_varname;
            while ismember(newname, result.Properties.VariableNames)
                newname = sprintf('%s_dup%d', factor_varname, k);
                k = k + 1;
            end
            factor_varname = newname;
        end

        col_vals = cell(length(attributes), 1);
        for j = 1:length(attributes)
            attr = attributes{j};
            val = T{i, attr};

            if iscell(val)
                val = val{1};
            end

            col_vals{j} = num2str(val);
        end

        result.(factor_varname) = col_vals;
    end
    
    factor_constraint = result;
end












