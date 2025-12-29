function append_log_to_file(log_file_or_fmt, varargin)

try
    % Determine whether first arg is a path or format string
    if nargin == 0
        return;
    end

    first = log_file_or_fmt;
    if ischar(first) && contains(first, filesep)
        % first is a path
        log_file = first;
        if nargin >= 2
            fmt = varargin{1};
            args = varargin(2:end);
        else
            return;
        end
    else

        script_dir = fileparts(mfilename('fullpath'));
        repo_root = fileparts(script_dir); 
        log_dir = fullfile(repo_root, '..', 'logs');
        if ~exist(log_dir, 'dir')
            try mkdir(log_dir); catch, end
        end
        try
            date_suffix = datestr(now, 'yyyymmdd');
        catch
            % Fallback in case now() isn't available for some reason
            date_suffix = datestr(datetime('now'), 'yyyymmdd');
        end
        log_file = fullfile(log_dir, ['weight_optimizer_' date_suffix '.log']);
       
        fmt = first;
        args = varargin;
    end

    % Format message
    try
        if ~isempty(args)
            msg = sprintf(fmt, args{:});
        else
            msg = fmt;
        end
    catch
        % If formatting fails, fallback to concatenation
        try
            msg = sprintf('%s', fmt);
        catch
            msg = '<log formatting error>';
        end
    end

    % Prepend timestamp and ensure newline
    try
        timestamp = datestr(now, 'yyyy-mm-dd HH:MM:SS');
        if isempty(msg) || msg(end) ~= char(10)
            msg = [msg char(10)];
        end
        out = sprintf('%s %s', timestamp, msg);
    catch
        out = [datestr(now) ' ' msg char(10)];
    end

    % Append to file using UTF-8 encoding; write BOM for newly created files
    try
        file_existed = exist(log_file, 'file') == 2;
        % Try opening with explicit UTF-8 encoding (MATLAB R2019b+ supports encoding argument)
        fid = -1;
        try
            fid = fopen(log_file, 'a', 'n', 'UTF-8');
        catch
            % Fall back to older fopen signature if encoding not supported
            try
                fid = fopen(log_file, 'a');
            catch
                fid = -1;
            end
        end
        if fid > 0
            if ~file_existed
                % write UTF-8 BOM to help editors detect encoding
                try
                    fwrite(fid, uint8([239 187 191]));
                catch
                    % ignore BOM write errors
                end
            end
            try
                fwrite(fid, out);
            catch
                % fallback to fprintf if fwrite fails for some reason
                try
                    fprintf(fid, '%s', out);
                catch
                    % ignore
                end
            end
            fclose(fid);
        end
    catch
        % ignore
    end
catch
    % swallow all errors to avoid impacting main flow
end
end
