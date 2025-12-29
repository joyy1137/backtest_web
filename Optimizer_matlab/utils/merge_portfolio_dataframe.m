function df_result = merge_portfolio_dataframe(output_path)
% merge_portfolio_dataframe - 从配置文件读取投资组合信息，合并优化后的数据并导出为CSV
%
% 用法:
%   df_result = merge_portfolio_dataframe(output_path)
%   df_result = merge_portfolio_dataframe()  % 使用当前工作目录作为导出路径
%
% 参数:
%   output_path - 导出CSV文件的路径（可选），如果未提供则使用当前工作目录

    % 如果未提供输出路径，使用当前工作目录
    if nargin < 1 || isempty(output_path)
        output_path = pwd;
    end
    
    output_path = char(output_path);
    
    % 检查输出路径是否存在，如果不存在则创建
    if ~exist(output_path, 'dir')
        try
            mkdir(output_path);
            fprintf_log('创建输出目录: %s\n', output_path);
        catch ME
            error('无法创建输出目录 %s: %s', output_path, ME.message);
        end
    end

    script_dir = fileparts(mfilename('fullpath'));
    % Ensure we have a repo-level logs directory and a file to append
    log_dir = fullfile(script_dir, '..', '..', 'logs');
    if ~exist(log_dir, 'dir')
        try
            mkdir(log_dir);
        catch
        end
    end
    % Add date suffix to log file so each day's run appends to a dated log

    try
        date_suffix = datestr(now, 'yyyymmdd');
    catch
        % Fallback in case now() isn't available for some reason
        date_suffix = datestr(datetime('now'), 'yyyymmdd');
    end
    log_file = fullfile(log_dir, ['weight_optimizer_' date_suffix '.log']);
    
    % 读取配置文件获取投资组合基础信息
    fprintf_log('正在读取配置文件: ');
   
    try
        
        [portfolio_info, ~, ~] = ConfigReader_sql();
        fprintf_log('从配置文件读取到 %d 个投资组合\n', height(portfolio_info));
           
      
    catch ME
        % Provide full error report for easier debugging
        try
            fullReport = getReport(ME, 'extended');
        catch
            fullReport = ME.message;
        end
        error('读取配置文件失败: %s\n%s', ME.message, fullReport);
    end
  
    
    % 提取投资组合信息
    portfolio_names = portfolio_info.portfolio_name;
    user_names = portfolio_info.user_name;
    start_dates = portfolio_info.start_date;
    end_dates = portfolio_info.end_date;
    
    % 将portfolio_names和user_names转换为cell数组
    if iscell(portfolio_names)
        portfolio_names_list = portfolio_names;
    else
        portfolio_names_list = {};
        for i = 1:length(portfolio_names)
            portfolio_names_list{end+1} = char(string(portfolio_names(i)));
        end
    end
    
    if iscell(user_names)
        user_names_list = user_names;
    else
        user_names_list = {};
        for i = 1:length(user_names)
            user_names_list{end+1} = char(string(user_names(i)));
        end
    end
    
    
    
    currentFile = mfilename('fullpath');
    currentDir = fileparts(currentFile);

    path_config = fullfile(currentDir, '..', '..','config', 'paths.yaml');

    path = ReadYaml(path_config);
    base_path = path.processing_data_dir;

    % 初始化结果table（用于收集所有数据）
    df_all = table();
    
    % 遍历每个投资组合
    for i = 1:length(portfolio_names_list)
        portfolio_name = portfolio_names_list{i};
        user_name = user_names_list{i};
        
        % 构建投资组合路径
        portfolio_path = fullfile(base_path, user_name, portfolio_name);
        
        % 检查路径是否存在
        if ~exist(portfolio_path, 'dir')
            fprintf_log('警告: 投资组合路径不存在: %s，跳过\n', portfolio_path);
            continue;
        end
        
    fprintf_log('\n处理投资组合: %s (%s)\n', portfolio_name, user_name);
        
        % 获取日期范围
        if iscell(start_dates)
            start_date_val = start_dates{i};
            end_date_val = end_dates{i};
        else
            start_date_val = start_dates(i);
            end_date_val = end_dates(i);
        end
        
        % 转换为datetime格式
        if ischar(start_date_val) || isstring(start_date_val)
            try
                start_date_dt = datetime(char(start_date_val), 'InputFormat', 'yyyy-MM-dd');
            catch
                start_date_dt = datetime(char(start_date_val));
            end
        elseif ~isdatetime(start_date_val)
            start_date_dt = datetime(start_date_val);
        else
            start_date_dt = start_date_val;
        end
        
        if ischar(end_date_val) || isstring(end_date_val)
            try
                end_date_dt = datetime(char(end_date_val), 'InputFormat', 'yyyy-MM-dd');
            catch
                end_date_dt = datetime(char(end_date_val));
            end
        elseif ~isdatetime(end_date_val)
            end_date_dt = datetime(end_date_val);
        else
            end_date_dt = end_date_val;
        end
        
        % 根据配置的日期范围生成工作日列表（不遍历目录）
        try
            % 将日期转换为字符串格式
            start_date_str = datestr(start_date_dt, 'yyyy-mm-dd');
            end_date_str = datestr(end_date_dt, 'yyyy-mm-dd');
            
            workday_table = WorkingDaysList(start_date_str, end_date_str);
            workday_list = workday_table{:,1};
            
            % 转换为字符串格式
            if isdatetime(workday_list)
                date_str_list = cellstr(datestr(workday_list, 'yyyy-mm-dd'));
            elseif iscell(workday_list)
                date_str_list = cellfun(@(x) char(string(x)), workday_list, 'UniformOutput', false);
            else
                date_str_list = cellstr(string(workday_list));
            end
        catch ME
            fprintf_log('  警告: 获取工作日列表失败: %s，跳过\n', ME.message);
            continue;
        end
        
        if isempty(date_str_list)
            fprintf_log('  警告: 投资组合 %s 在配置日期范围内没有工作日，跳过\n', portfolio_name);
            continue;
        end
        
    fprintf_log('  日期范围: %s 到 %s，共 %d 个工作日\n', ...
            datestr(start_date_dt, 'yyyy-mm-dd'), ...
            datestr(end_date_dt, 'yyyy-mm-dd'), ...
            length(date_str_list));
        
        % 只处理配置日期范围内的日期
        for d_idx = 1:length(date_str_list)
            valuation_date = date_str_list{d_idx};
            date_path = fullfile(portfolio_path, valuation_date);
            
            try
                % 检查必要文件是否存在
                code_file = fullfile(date_path, 'Stock_code.csv');
                weight_file = fullfile(date_path, 'weight.csv');
                
                if ~exist(code_file, 'file')
                    fprintf_log('  警告: %s/%s 缺少 Stock_code.csv，跳过\n', portfolio_name, valuation_date);
                    continue;
                end
                
                if ~exist(weight_file, 'file')
                    fprintf_log('  警告: %s/%s 缺少 weight.csv，跳过\n', portfolio_name, valuation_date);
                    continue;
                end
                
                % 读取Stock_code.csv（跳过第一行日期）
                df_code = readtable(code_file, 'ReadVariableNames', false, 'HeaderLines', 1);
                if isempty(df_code)
                    fprintf_log('  警告: %s/%s Stock_code.csv 为空，跳过\n', portfolio_name, valuation_date);
                    continue;
                end
                code = df_code{:, 1};
                
                % 读取权重数据
                df_weight = readtable(weight_file, 'ReadVariableNames', false);
                if isempty(df_weight)
                    fprintf_log('  警告: %s/%s weight.csv 为空，跳过\n', portfolio_name, valuation_date);
                    continue;
                end
                weight = df_weight{:, 1};
                
                % 验证长度一致
                if length(code) ~= length(weight)
                    fprintf_log('  警告: %s/%s 股票代码数量(%d)与权重数量(%d)不一致，跳过\n', ...
                        portfolio_name, valuation_date, length(code), length(weight));
                    continue;
                end
                
                % 转换为cell数组格式
                code = cellstr(string(code));
                valuation_date_cell = repmat({valuation_date}, length(code), 1);
                portfolio_name_cell = repmat({portfolio_name}, length(code), 1);
                
                % 确保weight是数值类型
                if ~isnumeric(weight)
                    weight = str2double(string(weight));
                end
                weight = weight(:);
                
                % 获取当前投资组合的 portfolio-level id 和 session_id（来自配置）
                lenCodes = length(code);
                if exist('portfolio_info', 'var') && ~isempty(portfolio_info)
                    if ismember('id', portfolio_info.Properties.VariableNames)
                        p_id_val = char(string(portfolio_info.id(i)));
                    else
                        p_id_val = '';
                    end
                    if ismember('session_id', portfolio_info.Properties.VariableNames)
                        p_session_val = char(string(portfolio_info.session_id(i)));
                    else
                        p_session_val = '';
                    end
                else
                    p_id_val = '';
                    p_session_val = '';
                end

                % 创建当前日期的table
                df_current = table();
                
                % 设置valuation_date列
                df_current.valuation_date = valuation_date_cell;
                
                % 设置code列
                df_current.code = code;
                
                % 设置portfolio_name列
                df_current.portfolio_name = portfolio_name_cell;

                % 设置portfolio级别 id 和 session_id（来自配置文件）
                df_current.id = repmat({p_id_val}, lenCodes, 1);
                df_current.session_id = repmat({p_session_val}, lenCodes, 1);
                
                % 设置weight列（Float类型）
                df_current.weight = double(weight);
                
                % 合并到总结果table（保留所有数据）
                if isempty(df_all)
                    df_all = df_current;
                else
                    df_all = [df_all; df_current];
                end
                
                catch ME
                fprintf_log('  错误: 处理 %s/%s 时出错: %s\n', portfolio_name, valuation_date, ME.message);
                continue;
            end
        end
    end
    
    % 按投资组合+日期组合导出并返回合并后的结果
    if ~isempty(df_all)
        % 获取唯一的投资组合和日期列表
        unique_portfolios = unique(df_all.portfolio_name);
        unique_dates = unique(df_all.valuation_date);
        
        % 确保是cell数组
        if iscell(unique_portfolios)
            portfolios_list = unique_portfolios;
        else
            portfolios_list = cellstr(string(unique_portfolios));
        end
        
        if iscell(unique_dates)
            dates_list = unique_dates;
        else
            dates_list = cellstr(string(unique_dates));
        end
        
        % 按每个投资组合+日期组合导出为单独的CSV文件
        for p_idx = 1:length(portfolios_list)
            current_portfolio = char(portfolios_list{p_idx});
            
            for date_idx = 1:length(dates_list)
                current_date = char(dates_list{date_idx});
                
                % 筛选当前投资组合和日期的数据
                portfolio_mask = strcmp(df_all.portfolio_name, current_portfolio);
                date_mask = strcmp(df_all.valuation_date, current_date);
                combined_mask = portfolio_mask & date_mask;
                df_export = df_all(combined_mask, :);
                
                if ~isempty(df_export)
                    
                    portfolio_name_clean = strrep(current_portfolio, '-', '_');
                    portfolio_name_clean = strrep(portfolio_name_clean, ' ', '_');
                    date_str_clean = strrep(current_date, '-', '');
                    session_id = df_export.session_id{1};
                    id = df_export.id{1};
                    folder_path = fullfile(output_path, session_id,id);
                    if ~exist(folder_path, 'dir')
                        mkdir(folder_path);
                    end

                    outFile = fullfile(folder_path, [portfolio_name_clean '_' date_str_clean '.csv']);

                    try
                        writetable(df_export, outFile);
                        fprintf_log('导出: %s\n', outFile);
                    catch ME
                        warning('导出失败 %s/%s: %s', current_portfolio, current_date, ME.message);
                    end
                end
            end
        end
        
      
        if nargout > 0
            df_result = df_all;
        end
        
    else
        if nargout > 0
            df_result = table();
        end
    end

end

function append_log_to_file(log_file, fmt, varargin)
    % Append a timestamped message to log_file. Swallow any errors to avoid
    % impacting the main processing flow.
    try
        fid = fopen(log_file, 'a');
        if fid > 0
            try
                timestamp = datestr(now, 'yyyy-mm-dd HH:MM:SS');
                if nargin > 1
                    msg = sprintf(fmt, varargin{:});
                else
                    msg = fmt;
                end
                % Ensure newline at end
                if isempty(msg) || msg(end) ~= char(10)
                    msg = [msg '\n'];
                end
                fprintf(fid, '%s %s', timestamp, msg);
            catch
                % ignore formatting/writing errors
            end
            fclose(fid);
        end
    catch
        % ignore file open errors
    end
end
