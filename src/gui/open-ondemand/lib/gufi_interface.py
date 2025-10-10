import subprocess
from dataclasses import dataclass
from typing import List
import gufi_config

""" This dataclass is used to represent a file or directory entry as returned by gufi_ls. """
@dataclass
class Node:
    file_type: str
    permissions: str
    links: str
    user: str
    group: str
    size: str
    last_mod_day: str
    last_mod_month: str
    last_mod_time: str
    name: str

def run_gufi_ls(path, options: List[str] = None) -> List[Node]:
    def parse_ls_stdout(stdout):
        lines = stdout.splitlines()
        nodes = []
        for line in lines:
            parts = line.split()
            if len(parts) < 9:
                continue
            permissionString = parts[0]
            file_type = permissionString[0]
            permissions = permissionString[1:]
            links = parts[1]
            user = parts[2]
            group = parts[3]
            size = parts[4]
            last_mod_month = parts[5]
            last_mod_day = parts[6]
            last_mod_time = parts[7]
            name = ' '.join(parts[8:])
            nodes.append(Node(file_type, permissions, links, user, group, size, last_mod_day, last_mod_month, last_mod_time, name))
        return nodes

    if options is None:
        options = ['-l', '-a']
    cmd = ['/usr/local/bin/gufi_ls'] + options + [path]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return parse_ls_stdout(result.stdout)
    except Exception:
        return []

gufi_find_help_text = {
        'amin': 'File was last accessed n minutes ago.',
        'atime': 'File was last accessed n*24 hours ago.',
        'cmin': "File's status was last changed n minutes ago.",
        'ctime': "File's status was last changed n*24 hours ago.",
        'empty': 'File is empty and is either a regular file or a directory.',
        'executable': 'Matches files which are executable and directories which are searchable (in a file name resolution sense).',
        # just do group 'gid': "File's numeric group ID is n.",
        'group': 'File belongs to group gname (numeric group ID allowed).',
        # remove for now'iname': 'Like name, but the match is case insensitive.',
        # remove for now 'inum': 'File has inode number n. It is normally easier to use the -samefile test instead.',
        'links': 'File has n links.',
        # remove for now 'lname': 'File is a symbolic link whose contents match shell pattern pattern.',
        'mmin': "File's data was last modified n minutes ago.",
        'mtime': "File's data was last modified n*24 hours ago.",
        'name': 'Base of file name (the path with the leading directories removed) matches shell pattern pattern.',
        'newer': 'File was modified more recently than file.',
        'path': 'File name matches shell pattern pattern.',
        'readable': 'Matches files which are readable.',
        'samefile': 'File refers to the same inode as name.',
        'size': 'File size.',
        'type': 'File is of type c.',
        # just do user'uid': "File's numeric user ID is n.",
        'user': 'File is owned by user uname (numeric user ID allowed).',
        'writable': 'Matches files which are writable.',
        'maxdepth': 'Descend at most levels (a non-negative integer) levels of directories below the command line arguments. -maxdepth 0 means only apply the tests and actions to the command line arguments.',
        'mindepth': 'Do not apply any tests or actions at levels less than levels (a non-negative integer). mindepth 1 means process all files except the command line arguments.',
        'size_percent': 'Modifier to the size flag. Expects 2 values that define the min and max percentage from the size.',
        # man says num_results when it's actually numresults, doesn't seem to work with other paramters, 'num_results': 'First n results.', 
        #'doesn't seem to work 'smallest': 'Top n smallest files.',
        # doesn't seem to work 'largest': 'Top n largest files.'
    }



def run_gufi_find(path_prefix: str = "", args: dict = None):
    
    cmd_args = []
    if path_prefix:
        cmd_args += ['-P', path_prefix]

    double_dash = {'num_results', 'smallest', 'largest'}
    boolean_flags = {'empty', 'executable', 'readable', 'writable'}
    

    # validate args and add to cmd_args
    for k, v in args.items():
        if k not in gufi_find_help_text:
            raise ValueError(f"Invalid gufi_find argument: {k}")
        if v not in ["None", None, '', False]:
            if k in boolean_flags:
                cmd_args.append(f'--{k}')
            elif k in double_dash:
                cmd_args.append(f'--{k}')
                cmd_args.append(str(v))
            else:
                cmd_args.append(f'-{k}')
                cmd_args.append(str(v))

    cmd = ['/usr/local/bin/gufi_find'] + cmd_args 
    # Note this doesn't work as expecte with num results+ ['--numresults', '100']

    try:
        print(cmd)
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        lines = result.stdout.strip().split('\n') if result.stdout else []
        return lines
    except Exception:
        print(f"Failed to run gufi_find with command: {' '.join(cmd)}")
        return []

def parse_stat_stdout(stdout):
    
    info = {}
    lines = stdout.splitlines()
    info['File'] = lines[0].split('File: ')[1]

    second_line_split = lines[1].split()
    info['Size'] = second_line_split[1]
    info['Blocks'] = second_line_split[3]
    info['IO Block'] = second_line_split[6]
    info['File type'] = second_line_split[7]

    third_line_split = lines[2].split()
    info['Device'] = third_line_split[1]
    info['Inode'] = third_line_split[5]
    info['Links'] = third_line_split[7]
    
   
    fourth_line_split = lines[3].split()
  
    info['Access'] = fourth_line_split[1]
    info['Uid'] = fourth_line_split[4].rstrip(')')
    info['Gid'] = fourth_line_split[7].rstrip(')')

    info['Modify'] = lines[6].split('Modify: ')[1]
    info['Change'] = lines[7].split('Change: ')[1]
    info['Birth'] = lines[8].split('Birth: ')[1] if 'Birth: ' in lines[8] else 'N/A'


    return info

def run_gufi_stat(path):
    cmd = ['/usr/local/bin/gufi_stat', path]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        x = parse_stat_stdout(result.stdout)
        return x
    except Exception:
        return None

