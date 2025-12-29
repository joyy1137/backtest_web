function batch_run_optimizer()  
    % 批量调用 optimizer_matlab_func_v2 的脚本
    % 根据配置文件中的 portfolio_info 批量优化多个组合
   

    script_dir = fileparts(mfilename('fullpath'));
    
    % 添加路径
    addpath(fullfile(script_dir, 'utils'));
    addpath(fullfile(script_dir, 'tools')); 
    addpath(fullfile(script_dir, 'config')); 
    
    % 读取配置文件
    

  

    try

        [portfolio_info, ~, ~] = ConfigReader_sql();
        
        fprintf_log('从配置文件读取到 %d 个投资组合\n', height(portfolio_info));
           
    catch ME
        warning(ME.identifier, '配置文件读取失败: %s，尝试遍历输出目录', ME.message);

        return;
    end
    
    % 获取投资组合信息
    portfolio_names = portfolio_info.portfolio_name;
    user_names = portfolio_info.user_name;
    start_dates = portfolio_info.start_date;
    end_dates = portfolio_info.end_date;
    
    currentFile = mfilename('fullpath');
    currentDir = fileparts(currentFile);

    path_config = fullfile(currentDir, '..','config', 'paths.yaml');

    path = ReadYaml(path_config);
    input_root = path.processing_data_dir;  % 用于读取优化后的数据
    %output_root = fullfile(script_dir, '..', 'output', 'optimization_results'); 
    
    % 遍历每个投资组合
    for i = 1:length(portfolio_names)
        pname = portfolio_names{i};
        uname = user_names{i};
        
    fprintf_log('开始优化投资组合: %s \n', pname);
        
        % 获取当前投资组合的日期范围
        current_start = start_dates(i);
        current_end = end_dates(i);
        
        % 转换日期格式
        if isdatetime(current_start)
            start_date_str = datestr(current_start, 'yyyy-mm-dd');
        else
            start_date_str = char(current_start);
        end
        
        if isdatetime(current_end)
            end_date_str = datestr(current_end, 'yyyy-mm-dd');
        else
            end_date_str = char(current_end);
        end
        
    fprintf_log('  日期范围: %s 到 %s\n', start_date_str, end_date_str);
        
        % 为当前投资组合获取工作日列表
        try
            workday_table = WorkingDaysList(start_date_str, end_date_str);
            workday_list = workday_table{:,1};
           
        catch ME
            warning(ME.identifier, '获取工作日列表失败: %s，跳过投资组合 %s', ME.message, pname);
            continue;
        end
        
        portfolio_path = fullfile(input_root, uname, pname);
        
        if ~exist(portfolio_path, 'dir')
            warning('投资组合目录不存在: %s，跳过', portfolio_path);
            continue;
        end
        
        % 遍历该投资组合的所有工作日
        for j = 1:length(workday_list)
            wday = workday_list(j);
            if isdatetime(wday)
                wday_str = datestr(wday, 'yyyy-mm-dd');
            else
                wday_str = char(wday);
            end
            
            input_path = fullfile(portfolio_path, wday_str);
            
            if ~exist(input_path, 'dir')
                fprintf_log('日期目录不存在: %s，跳过\n', input_path);
                continue;
            end
            
            % 检查必要的输入文件是否存在
            required_files = {'parameter_selecting.xlsx', 'Stock_risk_exposure.csv', ...
                             'Stock_score.csv', 'Stock_code.csv'};
            files_exist = true;
            for k = 1:length(required_files)
                if ~exist(fullfile(input_path, required_files{k}), 'file')
                    fprintf_log('缺少必要文件 %s，跳过 %s\n', required_files{k}, input_path);
                    files_exist = false;
                    break;
                end
            end
            
            if ~files_exist
                continue;
            end
            
            % 确定昨日路径（用于权重约束）
            if j > 1
                prev_wday = workday_list(j-1);
                if isdatetime(prev_wday)
                    prev_wday_str = datestr(prev_wday, 'yyyy-mm-dd');
                else
                    prev_wday_str = char(prev_wday);
                end
                yes_path = fullfile(portfolio_path, prev_wday_str);
            else
                try
                    prev_wday_str = LastWorkdayCalculator(wday_str);
                    yes_path = fullfile(portfolio_path, char(prev_wday_str));
                    
                    % 检查计算出的前一天路径是否存在
                    if ~exist(yes_path, 'dir')
                        fprintf_log('上一个工作日路径不存在: %s，使用当前路径作为参考\n', yes_path);
                        yes_path = input_path; % 如果找不到，使用当前路径作为参考
                    else
                        fprintf_log('使用上一个工作日路径: %s\n', yes_path);
                    end
                catch ME
                    fprintf_log('计算上一个工作日失败: %s，使用当前路径作为参考\n', ME.message);
                    yes_path = input_path; % 如果计算失败，使用当前路径作为参考
                end
            end
            
            try
                fprintf_log('正在优化: %s - %s\n', pname, wday_str);
                optimizer_matlab_func_v2(input_path, yes_path, 6);
                fprintf_log('优化完成: %s - %s\n', pname, wday_str);
            catch ME
                fprintf_log('优化失败: %s - %s\n', pname, wday_str);
                fprintf_log('错误信息: %s\n', ME.message);
                % 显示错误堆栈的前几层
                if length(ME.stack) > 0
                    fprintf_log('错误位置: %s (第 %d 行)\n', ME.stack(1).name, ME.stack(1).line);
                end
            end
        end
        
        fprintf_log('投资组合 %s 优化完成\n', pname);
    end
    
    fprintf_log('所有投资组合批量优化完成\n');
end

