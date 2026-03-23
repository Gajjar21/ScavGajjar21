"""GJ21 branded launcher for AWB Pipeline V3."""

from __future__ import annotations

import tkinter as tk
from pathlib import Path

from PIL import Image, ImageTk

from V3.ui.app_window import App

LOGO_FILE = Path(__file__).resolve().parent / "ui" / "assets" / "gj21_logo.png"


class Launcher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("GJ21 Launcher")
        self.geometry("460x300")
        self.resizable(False, False)
        self.configure(bg="#0b081d")
        self._logo_img = None
        self._build()

    def _build(self):
        card = tk.Frame(self, bg="#130a2f", bd=1, relief="solid", highlightthickness=0)
        card.pack(fill="both", expand=True, padx=18, pady=18)

        if LOGO_FILE.exists():
            try:
                img = Image.open(LOGO_FILE).convert("RGBA").resize((104, 104), Image.Resampling.LANCZOS)
                self._logo_img = ImageTk.PhotoImage(img)
                tk.Label(card, image=self._logo_img, bg="#130a2f").pack(pady=(24, 8))
                self.iconphoto(True, self._logo_img)
            except Exception:
                tk.Label(card, text="GJ21", fg="white", bg="#130a2f", font=("Arial", 34, "bold")).pack(pady=(24, 8))
        else:
            tk.Label(card, text="GJ21", fg="white", bg="#130a2f", font=("Arial", 34, "bold")).pack(pady=(24, 8))

        tk.Label(
            card,
            text="AWB Pipeline V3",
            fg="#f2f5f9",
            bg="#130a2f",
            font=("Arial", 16, "bold"),
        ).pack()
        tk.Label(
            card,
            text="Operations Control Centre",
            fg="#a3adbe",
            bg="#130a2f",
            font=("Arial", 11),
        ).pack(pady=(4, 16))

        tk.Button(
            card,
            text="Launch",
            command=self._launch,
            bg="#ff7a1a",
            fg="white",
            activebackground="#f06f12",
            activeforeground="white",
            relief="flat",
            padx=24,
            pady=8,
            font=("Arial", 11, "bold"),
            cursor="hand2",
        ).pack()

    def _launch(self):
        self.destroy()
        app = App()
        app.mainloop()


def main():
    launcher = Launcher()
    launcher.mainloop()


if __name__ == "__main__":
    main()
