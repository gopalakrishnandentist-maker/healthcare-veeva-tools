#!/usr/bin/env python3
"""
🔥 HCP Duplicate Check Tool - Modern UI Edition 🔥

A cool, modern GUI wrapper for the HCP Duplicate Check Tool.

Prereqs:
  pip install pandas openpyxl rapidfuzz

How to use:
  1) Put this file (hcp_dupe_gui.py) in the SAME folder as hcp_dupe_tool.py
  2) Run:  python hcp_dupe_gui.py
  3) Choose your file, configure settings, click Run!
"""

from __future__ import annotations

import os
import sys
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading

# Modern color scheme - Gen Z vibes 🎨
COLORS = {
    'bg_dark': '#1a1a2e',           # Deep navy background
    'bg_medium': '#16213e',         # Card background
    'accent': '#0f3460',            # Darker accent
    'primary': '#00d4ff',           # Bright cyan
    'secondary': '#ff6b9d',         # Pink accent
    'success': '#2ecc71',           # Green
    'warning': '#f39c12',           # Orange
    'error': '#e74c3c',             # Red
    'text': '#ffffff',              # White text
    'text_dim': '#a0a0a0',          # Dimmed text
    'hover': '#1f4068',             # Hover state
}

DEFAULT_SHARED_THRESHOLD = 5

def _python_exe() -> str:
    return sys.executable or "python"

def _tool_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    tool = os.path.join(here, "hcp_dupe_tool.py")
    return tool

class ModernButton(tk.Canvas):
    """Custom modern button with hover effects"""
    def __init__(self, parent, text, command, bg=COLORS['primary'], fg=COLORS['text'], 
                 width=200, height=45, **kwargs):
        super().__init__(parent, width=width, height=height, bg=COLORS['bg_dark'], 
                        highlightthickness=0, **kwargs)
        self.bg = bg
        self.fg = fg
        self.text = text
        self.command = command
        self.hover_bg = self._lighten_color(bg)
        
        # Draw rounded rectangle button
        self.rect = self.create_rounded_rect(2, 2, width-2, height-2, radius=22, fill=bg)
        self.text_id = self.create_text(width/2, height/2, text=text, fill=fg, 
                                       font=('Arial', 11, 'bold'))
        
        # Bind events
        self.bind('<Enter>', self._on_enter)
        self.bind('<Leave>', self._on_leave)
        self.bind('<Button-1>', self._on_click)
        
    def create_rounded_rect(self, x1, y1, x2, y2, radius=25, **kwargs):
        points = [
            x1+radius, y1,
            x2-radius, y1,
            x2, y1,
            x2, y1+radius,
            x2, y2-radius,
            x2, y2,
            x2-radius, y2,
            x1+radius, y2,
            x1, y2,
            x1, y2-radius,
            x1, y1+radius,
            x1, y1
        ]
        return self.create_polygon(points, smooth=True, **kwargs)
    
    def _lighten_color(self, hex_color):
        """Lighten a hex color for hover effect"""
        hex_color = hex_color.lstrip('#')
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        lighter = tuple(min(255, c + 30) for c in rgb)
        return f'#{lighter[0]:02x}{lighter[1]:02x}{lighter[2]:02x}'
    
    def _on_enter(self, e):
        self.itemconfig(self.rect, fill=self.hover_bg)
        self.config(cursor='hand2')
    
    def _on_leave(self, e):
        self.itemconfig(self.rect, fill=self.bg)
        self.config(cursor='')
    
    def _on_click(self, e):
        if self.command:
            self.command()

class ModernEntry(tk.Frame):
    """Custom modern entry field with label"""
    def __init__(self, parent, label, width=60, is_readonly=False, **kwargs):
        super().__init__(parent, bg=COLORS['bg_dark'], **kwargs)
        
        # Label
        tk.Label(self, text=label, bg=COLORS['bg_dark'], fg=COLORS['text_dim'],
                font=('Arial', 9)).pack(anchor='w', pady=(0, 4))
        
        # Entry container
        entry_frame = tk.Frame(self, bg=COLORS['accent'], bd=0)
        entry_frame.pack(fill='x')
        
        self.entry = tk.Entry(entry_frame, bg=COLORS['bg_medium'], fg=COLORS['text'],
                             font=('Arial', 10), bd=0, insertbackground=COLORS['primary'],
                             relief='flat', width=width)
        if is_readonly:
            self.entry.config(state='readonly')
        self.entry.pack(padx=2, pady=2, fill='x')
        
    def get(self):
        return self.entry.get()
    
    def set(self, value):
        self.entry.config(state='normal')
        self.entry.delete(0, tk.END)
        self.entry.insert(0, value)
        if self.entry.cget('state') == 'readonly':
            self.entry.config(state='readonly')

class HCPDupeGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("🔥 HCP Duplicate Checker")
        self.root.geometry("950x700")
        self.root.configure(bg=COLORS['bg_dark'])
        
        # Variables
        self.input_path = tk.StringVar(value="")
        self.outdir = tk.StringVar(value=os.path.expanduser("~/Desktop"))
        self.sheet_name = tk.StringVar(value="")
        self.shared_threshold = tk.StringVar(value=str(DEFAULT_SHARED_THRESHOLD))
        
        self.create_ui()
        
    def create_ui(self):
        # Main container with padding
        main = tk.Frame(self.root, bg=COLORS['bg_dark'])
        main.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Header
        self.create_header(main)
        
        # Content area
        content = tk.Frame(main, bg=COLORS['bg_dark'])
        content.pack(fill='both', expand=True, pady=20)
        
        # Input section
        self.create_input_section(content)
        
        # Settings section
        self.create_settings_section(content)
        
        # Action buttons
        self.create_action_section(content)
        
        # Log section
        self.create_log_section(content)
        
    def create_header(self, parent):
        header = tk.Frame(parent, bg=COLORS['bg_dark'])
        header.pack(fill='x', pady=(0, 10))
        
        title = tk.Label(header, text="🔥 HCP Duplicate Checker", 
                        bg=COLORS['bg_dark'], fg=COLORS['primary'],
                        font=('Arial', 24, 'bold'))
        title.pack(side='left')
        
        subtitle = tk.Label(header, text="Find & merge duplicate healthcare professionals",
                          bg=COLORS['bg_dark'], fg=COLORS['text_dim'],
                          font=('Arial', 11))
        subtitle.pack(side='left', padx=15, pady=8)
        
    def create_input_section(self, parent):
        section = tk.Frame(parent, bg=COLORS['bg_medium'], bd=0)
        section.pack(fill='x', pady=(0, 15))
        
        inner = tk.Frame(section, bg=COLORS['bg_medium'])
        inner.pack(padx=20, pady=20, fill='x')
        
        # Section title
        tk.Label(inner, text="📁 Input File", bg=COLORS['bg_medium'], 
                fg=COLORS['text'], font=('Arial', 13, 'bold')).pack(anchor='w', pady=(0, 15))
        
        # Input file
        input_frame = tk.Frame(inner, bg=COLORS['bg_medium'])
        input_frame.pack(fill='x', pady=(0, 10))
        
        self.input_entry = ModernEntry(input_frame, "Excel or CSV file with HCP data", width=70)
        self.input_entry.pack(side='left', fill='x', expand=True)
        self.input_entry.set(self.input_path.get())
        
        btn_browse = ModernButton(input_frame, "📂 Browse", 
                                 command=self.pick_input_file,
                                 width=120, height=40)
        btn_browse.pack(side='left', padx=(10, 0))
        
        # Sheet name (optional)
        sheet_frame = tk.Frame(inner, bg=COLORS['bg_medium'])
        sheet_frame.pack(fill='x')
        
        self.sheet_entry = ModernEntry(sheet_frame, "Sheet name (optional, leave blank for first sheet)", 
                                      width=40)
        self.sheet_entry.pack(side='left')
        self.sheet_entry.set(self.sheet_name.get())
        
    def create_settings_section(self, parent):
        section = tk.Frame(parent, bg=COLORS['bg_medium'], bd=0)
        section.pack(fill='x', pady=(0, 15))
        
        inner = tk.Frame(section, bg=COLORS['bg_medium'])
        inner.pack(padx=20, pady=20, fill='x')
        
        # Section title
        tk.Label(inner, text="⚙️ Settings", bg=COLORS['bg_medium'], 
                fg=COLORS['text'], font=('Arial', 13, 'bold')).pack(anchor='w', pady=(0, 15))
        
        settings_row = tk.Frame(inner, bg=COLORS['bg_medium'])
        settings_row.pack(fill='x')
        
        # Output directory
        self.outdir_entry = ModernEntry(settings_row, "Output folder", width=50)
        self.outdir_entry.pack(side='left', fill='x', expand=True)
        self.outdir_entry.set(self.outdir.get())
        
        btn_choose = ModernButton(settings_row, "📁 Choose", 
                                 command=self.pick_outdir,
                                 width=120, height=40)
        btn_choose.pack(side='left', padx=(10, 15))
        
        # Shared threshold
        threshold_frame = tk.Frame(settings_row, bg=COLORS['bg_medium'])
        threshold_frame.pack(side='left')
        
        self.threshold_entry = ModernEntry(threshold_frame, "Shared contact threshold", width=10)
        self.threshold_entry.pack()
        self.threshold_entry.set(self.shared_threshold.get())
        
        # Help text
        help_text = tk.Label(inner, 
                           text="💡 Contacts appearing in ≥ threshold VIDs won't auto-merge (prevents merging shared clinic phones/emails)",
                           bg=COLORS['bg_medium'], fg=COLORS['text_dim'],
                           font=('Arial', 9), wraplength=850, justify='left')
        help_text.pack(anchor='w', pady=(10, 0))
        
    def create_action_section(self, parent):
        section = tk.Frame(parent, bg=COLORS['bg_dark'])
        section.pack(fill='x', pady=(0, 15))
        
        # Run button - large and centered
        btn_run = ModernButton(section, "🚀 Run Duplicate Check", 
                              command=self.run_tool,
                              bg=COLORS['secondary'],
                              width=300, height=50)
        btn_run.pack()
        
    def create_log_section(self, parent):
        section = tk.Frame(parent, bg=COLORS['bg_medium'], bd=0)
        section.pack(fill='both', expand=True)
        
        inner = tk.Frame(section, bg=COLORS['bg_medium'])
        inner.pack(padx=20, pady=20, fill='both', expand=True)
        
        # Section title
        header = tk.Frame(inner, bg=COLORS['bg_medium'])
        header.pack(fill='x', pady=(0, 10))
        
        tk.Label(header, text="📊 Process Log", bg=COLORS['bg_medium'], 
                fg=COLORS['text'], font=('Arial', 13, 'bold')).pack(side='left')
        
        # Log text area
        log_frame = tk.Frame(inner, bg=COLORS['accent'])
        log_frame.pack(fill='both', expand=True)
        
        self.log_text = tk.Text(log_frame, bg=COLORS['bg_dark'], fg=COLORS['text'],
                               font=('Consolas', 9), bd=0, relief='flat',
                               insertbackground=COLORS['primary'], wrap='word')
        self.log_text.pack(side='left', fill='both', expand=True, padx=2, pady=2)
        
        scrollbar = tk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side='right', fill='y')
        self.log_text.config(yscrollcommand=scrollbar.set)
        
        # Initial message
        self.log_text.insert('1.0', "👋 Ready to find duplicates! Select your input file and click Run.\n")
        self.log_text.config(state='disabled')
        
    def pick_input_file(self):
        path = filedialog.askopenfilename(
            title="Select HCP export file",
            filetypes=[
                ("Excel files", "*.xlsx *.xlsm *.xls"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.input_path.set(path)
            self.input_entry.set(path)
            self.log(f"✅ Selected input: {os.path.basename(path)}\n", COLORS['success'])
            
    def pick_outdir(self):
        path = filedialog.askdirectory(title="Select output folder")
        if path:
            self.outdir.set(path)
            self.outdir_entry.set(path)
            self.log(f"✅ Output folder: {path}\n", COLORS['success'])
            
    def log(self, message, color=COLORS['text']):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, message)
        # Color the last inserted text
        last_line_start = self.log_text.index("end-1c linestart")
        self.log_text.tag_add("colored", last_line_start, "end-1c")
        self.log_text.tag_config("colored", foreground=color)
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')
        self.log_text.update_idletasks()
        
    def run_tool(self):
        tool = _tool_path()
        if not os.path.exists(tool):
            messagebox.showerror(
                "Missing tool file",
                "Couldn't find hcp_dupe_tool.py in the same folder as this GUI.\n"
                "Place hcp_dupe_gui.py and hcp_dupe_tool.py together, then try again.",
            )
            return

        input_path = self.input_entry.get().strip()
        if not input_path or not os.path.exists(input_path):
            messagebox.showerror("Input missing", "Please choose a valid input file.")
            return

        outdir = self.outdir_entry.get().strip()
        if not outdir:
            messagebox.showerror("Output folder missing", "Please choose an output folder.")
            return

        try:
            threshold = int(self.threshold_entry.get().strip())
            if threshold < 2:
                raise ValueError
        except Exception:
            messagebox.showerror("Invalid threshold", "Shared contact threshold must be an integer ≥ 2.")
            return

        sheet = self.sheet_entry.get().strip()

        # Run in thread to prevent UI freeze
        thread = threading.Thread(target=self._run_tool_thread, 
                                 args=(tool, input_path, sheet, outdir, threshold))
        thread.daemon = True
        thread.start()
        
    def _run_tool_thread(self, tool, input_path, sheet, outdir, threshold):
        cmd = [
            _python_exe(),
            tool,
            "--input", input_path,
            "--outdir", outdir,
            "--shared-threshold", str(threshold),
        ]
        if sheet:
            cmd += ["--sheet", sheet]

        self.log_text.config(state='normal')
        self.log_text.delete("1.0", tk.END)
        self.log_text.config(state='disabled')
        
        self.log("🚀 Starting duplicate check...\n", COLORS['primary'])
        self.log(f"📝 Command: {' '.join(cmd)}\n\n", COLORS['text_dim'])
        self.log("─" * 100 + "\n", COLORS['text_dim'])

        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            output = proc.stdout or ""
            
            # Parse output for better formatting
            for line in output.split('\n'):
                if 'Error' in line or 'ERROR' in line:
                    self.log(line + '\n', COLORS['error'])
                elif 'Warning' in line or 'WARNING' in line:
                    self.log(line + '\n', COLORS['warning'])
                elif 'Done' in line or 'SUCCESS' in line:
                    self.log(line + '\n', COLORS['success'])
                else:
                    self.log(line + '\n', COLORS['text'])

            self.log("\n" + "─" * 100 + "\n", COLORS['text_dim'])
            
            if proc.returncode == 0:
                self.log("✅ Duplicate check completed successfully!\n", COLORS['success'])
                self.root.after(0, lambda: messagebox.showinfo(
                    "Success! 🎉",
                    f"Duplicate check completed!\n\n"
                    f"📁 Outputs written to:\n{outdir}\n\n"
                    f"🔑 Key file: Dupe_Check_Outputs.xlsx",
                ))
            else:
                self.log("❌ Tool finished with errors. Check the log above.\n", COLORS['error'])
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    "The tool finished with errors.\n\n"
                    "Check the log output for details."
                ))
        except Exception as e:
            self.log(f"❌ Execution failed: {e}\n", COLORS['error'])
            self.root.after(0, lambda: messagebox.showerror(
                "Execution failed", 
                f"Could not run the tool.\n\n{e}"
            ))

def main():
    root = tk.Tk()
    app = HCPDupeGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
