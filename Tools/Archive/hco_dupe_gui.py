#!/usr/bin/env python3
"""
Small GUI wrapper for the HCO Duplicate Check Tool.

Prereqs:
  pip install pandas openpyxl
  pip install rapidfuzz   # optional but recommended

How to use:
  1) Put this file (hco_dupe_gui.py) in the SAME folder as hco_dupe_tool.py
  2) Run:  python3 hco_dupe_gui.py
  3) Choose input Excel/CSV, choose output folder, optionally select sheet, click Run.

Note:
  - This GUI calls hco_dupe_tool.py under the hood.
"""

from __future__ import annotations
import os
import sys
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox

DEFAULT_SHARED_THRESHOLD = 5

def _python_exe() -> str:
    return sys.executable or "python3"

def _tool_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "hco_dupe_tool.py")

def pick_input_file(var: tk.StringVar) -> None:
    path = filedialog.askopenfilename(
        title="Select HCO raw export file",
        filetypes=[
            ("Excel files", "*.xlsx *.xlsm *.xls"),
            ("CSV files", "*.csv"),
            ("All files", "*.*"),
        ],
    )
    if path:
        var.set(path)

def pick_outdir(var: tk.StringVar) -> None:
    path = filedialog.askdirectory(title="Select output folder")
    if path:
        var.set(path)

def run_tool(input_path: str, sheet: str, outdir: str, shared_threshold: int, log_widget: tk.Text) -> None:
    tool = _tool_path()
    if not os.path.exists(tool):
        messagebox.showerror(
            "Missing tool file",
            "Couldn't find hco_dupe_tool.py in the same folder as this GUI.\n"
            "Place hco_dupe_gui.py and hco_dupe_tool.py together, then try again.",
        )
        return
    if not input_path or not os.path.exists(input_path):
        messagebox.showerror("Input missing", "Please choose a valid input file.")
        return
    if not outdir:
        messagebox.showerror("Output folder missing", "Please choose an output folder.")
        return

    cmd = [
        _python_exe(), tool,
        "--input", input_path,
        "--outdir", outdir,
        "--shared-threshold", str(shared_threshold),
    ]
    if sheet.strip():
        cmd += ["--sheet", sheet.strip()]

    log_widget.configure(state="normal")
    log_widget.delete("1.0", tk.END)
    log_widget.insert(tk.END, "Running:\n" + " ".join(cmd) + "\n\n")
    log_widget.insert(tk.END, "----------------------------------------\n")
    log_widget.configure(state="disabled")
    log_widget.update_idletasks()

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        output = proc.stdout or ""
        log_widget.configure(state="normal")
        log_widget.insert(tk.END, output + "\n")
        log_widget.configure(state="disabled")

        if proc.returncode == 0:
            messagebox.showinfo(
                "Done",
                "HCO duplicate check completed.\n\n"
                "Outputs written to:\n" + outdir + "\n\n"
                "Key file: HCO_Dupe_Check_Outputs.xlsx",
            )
        else:
            messagebox.showerror(
                "Tool error",
                "The tool finished with an error.\n\n"
                "Check the log output in the GUI for details."
            )
    except Exception as e:
        messagebox.showerror("Execution failed", f"Could not run the tool.\n\n{e}")

def main() -> None:
    root = tk.Tk()
    root.title("HCO Duplicate Check — Run Tool")
    root.geometry("880x520")

    input_var = tk.StringVar(value="")
    outdir_var = tk.StringVar(value=os.path.expanduser("~/Desktop"))
    sheet_var = tk.StringVar(value="Sheet0")
    shared_var = tk.StringVar(value=str(DEFAULT_SHARED_THRESHOLD))

    frm = tk.Frame(root, padx=12, pady=12)
    frm.pack(fill="both", expand=True)

    tk.Label(frm, text="Input file (HCO raw export):").grid(row=0, column=0, sticky="w")
    tk.Entry(frm, textvariable=input_var, width=80).grid(row=0, column=1, sticky="we", padx=(8, 8))
    tk.Button(frm, text="Browse…", command=lambda: pick_input_file(input_var)).grid(row=0, column=2, sticky="e")

    tk.Label(frm, text="Sheet name (optional):").grid(row=1, column=0, sticky="w", pady=(8, 0))
    tk.Entry(frm, textvariable=sheet_var, width=25).grid(row=1, column=1, sticky="w", padx=(8, 8), pady=(8, 0))
    tk.Label(frm, text="(Leave blank for first sheet)").grid(row=1, column=1, sticky="w", padx=(220, 0), pady=(8, 0))

    tk.Label(frm, text="Output folder:").grid(row=2, column=0, sticky="w", pady=(8, 0))
    tk.Entry(frm, textvariable=outdir_var, width=80).grid(row=2, column=1, sticky="we", padx=(8, 8), pady=(8, 0))
    tk.Button(frm, text="Choose…", command=lambda: pick_outdir(outdir_var)).grid(row=2, column=2, sticky="e", pady=(8, 0))

    tk.Label(frm, text="Shared phone threshold (>=):").grid(row=3, column=0, sticky="w", pady=(8, 0))
    tk.Entry(frm, textvariable=shared_var, width=10).grid(row=3, column=1, sticky="w", padx=(8, 8), pady=(8, 0))
    tk.Label(frm, text="(If a phone appears in ≥ this many HCOs, it won't auto-merge)").grid(
        row=3, column=1, sticky="w", padx=(80, 0), pady=(8, 0)
    )

    def on_run():
        try:
            st = int(shared_var.get().strip())
            if st < 2:
                raise ValueError
        except Exception:
            messagebox.showerror("Invalid threshold", "Shared phone threshold must be an integer ≥ 2.")
            return
        run_tool(input_var.get().strip(), sheet_var.get().strip(), outdir_var.get().strip(), st, log_text)

    tk.Button(frm, text="Run HCO Duplicate Check", command=on_run, height=2).grid(row=4, column=1, sticky="w", padx=(8, 0), pady=(12, 8))

    tk.Label(frm, text="Log:").grid(row=5, column=0, sticky="nw")
    log_text = tk.Text(frm, height=18, wrap="word")
    log_text.grid(row=5, column=1, columnspan=2, sticky="nsew", padx=(8, 0))
    log_text.configure(state="disabled")

    frm.grid_columnconfigure(1, weight=1)
    frm.grid_rowconfigure(5, weight=1)

    root.mainloop()

if __name__ == "__main__":
    main()
