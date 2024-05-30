from Load_DF import load_df_from_dir,sanitize
from datetime import datetime
def read_datas():

    layouts = load_df_from_dir()

    for layout in layouts:
        layout['modified_date'] = datetime.now()
    layout_copies = []
    for layout in layouts:
        try:
            if layout is not None:
                sanitized_layout = sanitize(layout.copy())
                layout_copies.append(sanitized_layout)
            else:
                layout_copies.append(None)
        except Exception as e:
            print(f"An error occurred while sanitizing a layout: {e}")
            layout_copies.append(None)

    for i in range(len(layout_copies)):
        layout_copies[i] = sanitize(layout_copies[i])

    return layouts,layout_copies

