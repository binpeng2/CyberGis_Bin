import torch
import torch.nn as nn


class RNN(nn.Module):
    def __init__(self, input_size, hidden_size, output_size, model_type="rnn", n_layers=1):
        super().__init__()

        self.model_type = model_type
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.output_size = output_size
        self.n_layers = n_layers
        self.n_steps = 50

        if model_type == "lstm":
            self.rnn = nn.LSTM(hidden_size, hidden_size, n_layers)
        else:
            self.rnn = nn.RNN(hidden_size, hidden_size, n_layers, nonlinearity='relu')
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, input_data, hidden):

        batch_size, *_ = input_data.size()
        output, hidden = self.rnn(input_data.reshape(1, batch_size, -1), hidden)
        output = self.fc(output.reshape(batch_size, -1))

        return output, hidden

    def init_hidden(self, batch_size, device=None):

        hidden = (
            torch.zeros(self.n_layers, batch_size, self.hidden_size, device=device),
            torch.zeros(self.n_layers, batch_size, self.hidden_size, device=device)
        )

        return hidden