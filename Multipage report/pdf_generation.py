from datetime import datetime

from reportlab.platypus import SimpleDocTemplate, BaseDocTemplate, Table, TableStyle, Paragraph, Frame, Spacer, \
    PageTemplate, Indenter
from reportlab.lib import colors
from reportlab.lib.units import cm, inch
from reportlab.lib.pagesizes import A3, A4, landscape, portrait
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfgen import canvas

#
# def draw_static(canvas, doc):
#     # Save the current settings
#     canvas.saveState()
#
#     # Draw the static stuff
#     canvas.setFont('Times-Bold', 12, leading=None)
#     canvas.drawCentredString(200, 200, "This will be drawn on every page")
#
#     # Restore setting to before function call
#     canvas.restoreState()
#
#
# def textsize(canvas):
#     from reportlab.lib.units import inch
#     from reportlab.lib.colors import magenta, red
#     canvas.setFont("Times-Roman", 20)
#     canvas.setFillColor(red)
#     canvas.drawCentredString(2.75 * inch, 2.5 * inch, "Font size examples")
#     canvas.setFillColor(magenta)
#     size = 7
#     y = 2.3 * inch
#     x = 1.3 * inch
#     for line in ['abc', 'def']:
#         canvas.setFont("Helvetica", size)
#     canvas.drawRightString(x, y, "%s points: " % size)
#     canvas.drawString(x, y, line)
#     y = y - size * 1.2
#     size = size + 1.5


'''
Creating a flowable object
'''
# container for the "Flowable" objects
elements = []
styles = getSampleStyleSheet()
styleN = styles["Normal"]
styleB = styles["BodyText"]
styleB.alignment = TA_LEFT
# Make heading for each column and start data list
column_1 = "User Group Name"
column_2 = "Access Right"

# Assemble data for each column using simple loop to append it into data list
data = [[column_1, column_2]]

description = Paragraph('This is a very long paragraph and should be automatically wrapped around based on the size/width of the cell', styleB)
permission_data = (('Admin', description),
                   ('ID management', 'READ Portal_users'))
for i in range(len(permission_data)):
    user_group, access_rights = permission_data[i]
    data.append([user_group, access_rights])

for i in range(0, 20):
    data.append([str(i), "{}\n{}\n{}".format(str(i), str(i), str(i))])

table_split_over_pages = Table(data, [8 * cm, 6 * cm], repeatRows=1)
table_split_over_pages.hAlign = 'LEFT'
# (Attribute, (x1, y1), (x2, y2), args)
table_style = TableStyle([('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                          ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                          ('LINEBELOW', (0, 0), (-1, -1), 1, colors.black),
                          ('BOX', (0, 0), (-1, -1), 1, colors.black),
                          ('BOX', (0, 0), (0, -1), 1, colors.black)])
table_style.add('BACKGROUND', (0, 0), (1, 0), colors.lightblue)
table_style.add('BACKGROUND', (0, 1), (-1, -1), colors.white)
table_split_over_pages.setStyle(table_style)

'''
Template setup
'''
# Set up a basic template
doc = BaseDocTemplate('test.pdf', pagesize=A4)  # use landscape(A4) for landscape

# Create a Frame for the Flowables (Paragraphs and such)
frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='normal')

# Adding the frame to the template, an onPage attribute can be used if there's a method to be run on each page
template = PageTemplate(id='test', frames=[frame])

# Add the template to the doc
doc.addPageTemplates([template])

# All the default stuff for generating a document
story = []
'''
Printing the top portion of the report
'''
# title_paragraph
# top_section
# subtitle_paragraph
# table contents

#  Title
title = "<b>COMPANY NAME OVER HERE (SINGAPORE)</b>"

#  Top Section
top_section_data = [['PROJECT TITLE:', 'PROJECT TITLE'],
                    ['REPORT TYPE:', 'LOG REPORT'],
                    ['DATE:', '{}'.format(datetime.today().date())]]
top_section = Table(top_section_data, [4 * cm, 6 * cm], hAlign='LEFT')
top_section.setStyle(TableStyle([
    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
    ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold')]))

# Subtitle Paragraph
subtitle = "<b><u>THIS WILL BE THE TABLE DATA</u></b>"
# paragraph = Paragraph(title, styleN)
title_paragraph = Paragraph(title, styles["Normal"])
subtitle_paragraph = Paragraph(subtitle, styles["Normal"])

spacer = Spacer(0, 0.1 * inch)

# date report is generated
start_date = datetime.today().date()
end_date = datetime.today().date()
date_report_data = [['REPORT PERIOD:', '{start} TO {end} (INCLUSIVE)'.format(start=start_date, end=end_date)]]
date_report_section = Table(date_report_data, [4 * cm, 6 * cm], hAlign='LEFT')
date_report_section.setStyle(TableStyle([
    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
    ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold')]))
'''
Building the PDF story flow
'''
story.append(title_paragraph)

story.extend([Indenter(left=-6), top_section, Indenter(left=6)])
# story.append(top_section)
# story.append(Indenter(left=6))
story.append(spacer)
story.append(subtitle_paragraph)
story.append(spacer)
story.extend([Indenter(left=-6), date_report_section, Indenter(left=6)])
story.append(spacer)
story.append(table_split_over_pages)
doc.build(story)
