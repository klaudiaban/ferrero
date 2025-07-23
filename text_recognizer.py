import argparse
from pathlib import Path

import pdfplumber
import pytesseract
import pypdfium2 as pdfium
from PIL import Image


def extract_text_pdfplumber(pdf_path: Path) -> str:
    """Try to read embedded text from the PDF."""
    with pdfplumber.open(pdf_path) as pdf:
        texts = [(page.extract_text() or "").strip() for page in pdf.pages]
    return "\n".join(texts).strip()


def extract_text_ocr_pdfium(pdf_path: Path, lang: str = "eng+pol", scale: float = 2.0) -> str:
    """
    Render each page with pypdfium2 -> PIL Image -> run Tesseract OCR.
    scale ~ zoom factor (1.0 = 72dpi). 2.0–3.0 often gives better OCR.
    """
    pdf = pdfium.PdfDocument(pdf_path)
    all_text = []

    for i in range(len(pdf)):
        page = pdf[i]
        bitmap = page.render(scale=scale, rotation=0)
        pil_img = bitmap.to_pil()
        txt = pytesseract.image_to_string(pil_img, lang=lang)
        all_text.append(txt.strip())

    return "\n".join(all_text).strip()


def read_pdf_text(
    pdf_path: Path,
    force_ocr: bool = False,
    lang: str = "eng+pol",
    scale: float = 2.0,
    min_text_len: int = 50
) -> str:
    """
    Returns extracted text from a PDF:
    - Try pdfplumber first (unless force_ocr=True).
    - If text is too short, fall back to OCR with pypdfium2.
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"File not found: {pdf_path}")

    if not force_ocr:
        text = extract_text_pdfplumber(pdf_path)
        if len(text) >= min_text_len:
            return text

    # OCR fallback
    return extract_text_ocr_pdfium(pdf_path, lang=lang, scale=scale)


def main():
    parser = argparse.ArgumentParser(description="PDF text recognizer using pdfplumber + pypdfium2 OCR fallback")
    parser.add_argument("pdf", help="Path to PDF file")
    parser.add_argument("--force-ocr", action="store_true", help="Force OCR even if text is found")
    parser.add_argument("--lang", default="eng+pol", help="Tesseract language codes (e.g. 'eng', 'pol', 'eng+pol')")
    parser.add_argument("--scale", type=float, default=2.0, help="Rendering scale for OCR (1.0-3.0 typical)")
    parser.add_argument("--min-text-len", type=int, default=50, help="Threshold length to skip OCR")
    parser.add_argument("--outfile", default="", help="Save extracted text to this file")
    parser.add_argument("--tesseract-path", default="", help="Absolute path to tesseract.exe (Windows)")

    args = parser.parse_args()

    if args.tesseract_path:
        pytesseract.pytesseract.tesseract_cmd = args.tesseract_path

    pdf_path = Path(args.pdf)
    text = read_pdf_text(
        pdf_path=pdf_path,
        force_ocr=args.force_ocr,
        lang=args.lang,
        scale=args.scale,
        min_text_len=args.min_text_len
    )

    if args.outfile:
        out = Path(args.outfile)
        out.write_text(text, encoding="utf-8")
        print(f"[OK] Text saved to: {out.resolve()}")
    else:
        print(text)


if __name__ == "__main__":
    main()
