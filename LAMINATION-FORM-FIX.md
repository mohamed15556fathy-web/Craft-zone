# Lamination / breaking form calculation fix

- Lamination now uses the linked breaking form construction mode and piece count, the same way breaking does.
- For `pieceByPiece`, two pieces are required per finished product unless the form produces multiple pieces per strike.
- Add Order and Calculate Order previews use the same runs calculation as the server.
- If breaking is disabled, a previously selected form is not used for lamination preview.
