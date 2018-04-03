# Neil McGlohon -- Rensselaer Polytechnic Institute (c) 2018

import sys
from subprocess import call
from abc import ABC, abstractmethod


class CodesSim(object):
    def __init__(self, sim_name='codes_sim', bin_path=None, codes_config_path=None, working_dir_path=None, sync=1, num_procs=1, lookahead=1, extramem=0, workload_name=None, workload_type=None, workload_conf_file=None, alloc_file_path=None, lp_io_dir=None, other_options=None):
        self.sim_name = sim_name

        self.bin_path = bin_path
        self.codes_config_path = codes_config_path
        self.working_dir_path = CodesSim.format_path(working_dir_path)

        self.sync = sync
        self.num_procs = num_procs
        self.lookahead = lookahead
        self.extramem = extramem

        self.workload_type = workload_type
        self.workload_conf_file = workload_conf_file

        self.alloc_file_path = alloc_file_path

        self.lp_io_dir = lp_io_dir
        self.lp_io_use_suffix = 1

        if other_options is None:
            self.other_options = []
        else:
            self.other_options = other_options

        self.workload_manager = None

        self.call_str = None

    @staticmethod
    def format_path(path_str):
        if path_str != None:
            if path_str[-1] != '/':
                path_str += '/'
        return path_str

    def get_call_str(self):
        if self.call_str != None:
            return self.call_str
        else:
            self.call_str = 'mpirun -np %d %s --synch=%d --cons-lookahead=%d --workload_type=online --extramem=%d --workload_conf_file=%s --lp-io-dir=%s --lp-io-use-suffix=1 --alloc_file=%s' %(self.num_procs, self.bin_path, self.sync, self.lookahead, self.extramem, self.workload_conf_file, self.lp_io_dir, self.alloc_file_path)
            for option in self.other_options:
                self.call_str += ' %s'%option

            self.call_str += ' -- %s'%self.codes_config_path
            return self.call_str

    def load_workload_manager(self, wlm):
        self.workload_manager = wlm

    def run_sim(self):
        call_str = self.get_call_str()
        call(call_str, shell=True)


class Allocator(object):
    def __init__(self, alloc_script_path, out_file_path, alloc_method, num_terms, workloads):
        self.alloc_script_path = alloc_script_path
        self.alloc_out_file_path = out_file_path
        self.alloc_method = alloc_method
        self.num_terminals = num_terms
        self.workloads = workloads

    def allocate(self):
        print("Running Allocation Script...\nAlloc Config:")
        alloc_config_string = "%s\n%s\n"%(self.alloc_method, self.num_terminals)
        for wl in self.workloads:
            alloc_config_string += "%d "%wl.ranks
        with open("temp.conf", "w") as f:
            f.write(alloc_config_string)

        print(alloc_config_string + '\n')

        alloc_call_str = 'python %s temp.conf %s > /dev/null'%(self.alloc_script_path, self.alloc_out_file_path)
        call(alloc_call_str, shell=True)
        call('rm temp.conf', shell=True)

class WorkloadManager(object):
    def __init__(self):
        self.workload_conf_filename = "workload_temp.conf"
        self.workloads = []

    def set_workload_conf_filename(self, filename):
        self.workload_conf_filename = filename

    def add_workload(self, wl):
        self.workloads.append(wl)

    def get_workloads(self):
        return self.workloads

    def write_config(self, out_file_path):
        with open(out_file_path, 'w') as f:
            for wl in self.workloads:
                f.write("%s %s\n"%(wl.ranks, wl.workload_name))

class Workload(ABC):
    @abstractmethod
    def __init__(self):
        self.workload_name = "base"
        self.workload_type = "notype"
        self.ranks = 0

    @staticmethod
    def factory(workload_name, ranks=None):
        if ranks is not None:
            if workload_name == "lammps": return Lammps_Workload(ranks)
            if workload_name == "nekbone": return Nekbone_Workload(ranks)
            if workload_name == "synth": return Synthetic_Workload(ranks)
        if ranks is None: #we don't want to pass None to the constructor and end up ignoring the default value
            if workload_name == "lammps": return Lammps_Workload()
            if workload_name == "nekbone": return Nekbone_Workload()
            if workload_name == "synth": return Synthetic_Workload()

class Synthetic_Workload(Workload):
    def __init__(self, ranks=1024):
        self.workload_name = 'synthetic1' #random uniform
        self.ranks = ranks

class Lammps_Workload(Workload):
    def __init__(self, ranks=1024):
        self.workload_name = 'lammps'
        self.workload_type = 'online'
        self.ranks = ranks

class Nekbone_Workload(Workload):
    def __init__(self, ranks=1000):
        self.workload_name = 'nekbone'
        self.workload_type = 'online'
        self.ranks = ranks
