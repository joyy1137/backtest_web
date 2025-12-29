function data_preparation()
% 协调各个模块完成整个流程

   

    script_dir = fileparts(mfilename('fullpath'));

    addpath(fullfile(script_dir, 'utils'));
    addpath(fullfile(script_dir, 'tools')); 
    addpath(fullfile(script_dir, 'config')); 

    
    try

        [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReader_sql();

        fprintf_log('从配置文件读取到 %d 个投资组合\n', height(portfolio_info));
    
    catch ME
        warning(ME.identifier, '配置文件读取失败: %s，尝试遍历输出目录', ME.message);
        
        
        return;
    end
    

    % 获取稳定数据
    [df_st, df_stockuniverse] = StableData();

    % 遍历 portfolio_info 里的 portfolio_name
    portfolio_names = portfolio_info.portfolio_name;
    user_names = portfolio_info.user_name;
    start_dates = portfolio_info.start_date;
    end_dates = portfolio_info.end_date;
    


    for i = 1:length(portfolio_names)
        pname = portfolio_names{i};
        uname = user_names{i};
        
    fprintf_log('\n处理投资组合: %s \n', pname);
        
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
        
        % 处理约束参数
        try
            [upper_tbl, lower_tbl, style_list, industry_list, turnover_val, score_weight, sharpe_weight, constraint_mode, up_params, low_params] = ConstraintProcessor(pname, factor_constraint, portfolio_constraint);
        catch ME
            warning('未找到 %s 的upper或lower列', pname);
            continue;
        end

        % 遍历工作日
        for j = 1:length(workday_list)
            wday = workday_list(j);
            if isdatetime(wday)
                wday_str = datestr(wday, 'yyyy-mm-dd');
            else
                wday_str = char(wday);
            end
            
            % 数据准备阶段的输出路径（用于后续优化和回测的输入）
      
        
            currentFile = mfilename('fullpath');
            currentDir = fileparts(currentFile);

            path_config = fullfile(currentDir, '..','config', 'paths.yaml');

            path = ReadYaml(path_config);
          

            outdir = fullfile(path.processing_data_dir, uname, pname, wday_str);
            
            try
                % 获取投资组合信息
                row_idx = find(strcmp(portfolio_info.portfolio_name, pname), 1);
                info_row = portfolio_info(row_idx, :);
                
                data_date = LastWorkdayCalculator(wday_str);
                
                % 处理数据
                dbc = DatabaseConnector();
                [code_list, score_vec, init_weight, df_score, df_index, exposure_mat, specific_vec, factor_cov_mat, factor_names, ~] = DataProcessor(data_date, info_row.score_type{1}, info_row.index_type{1}, info_row.mode_type{1}, dbc, df_st, df_stockuniverse);
                
                stock_number = length(code_list);
                quantiles = [0.9 0.8 0.7 0.6 0.5 0.4 0.3 0.2 0];
                
                % 生成输出文件
                OutputGenerator(outdir, data_date, upper_tbl, lower_tbl, style_list, industry_list, ...
                    portfolio_constraint, info_row, turnover_val, score_weight, sharpe_weight, ...
                    code_list, score_vec, init_weight, df_score, df_index, stock_number, quantiles, ...
                    up_params, low_params, constraint_mode, exposure_mat, factor_names, specific_vec, ...
                    factor_cov_mat, info_row.index_type{1}, df_st);
                    
            catch ME
                warning(ME.identifier, '%s', ME.message);
            end
            disp(['完成 ', pname, ' 在 ', wday_str, ' 的数据处理']);
        end
    end
end
