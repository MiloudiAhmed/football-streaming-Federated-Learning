"""
fl_model.py
===========
Simple PyTorch neural network that predicts:
  "What is the probability of a goal in the next 10 minutes?"

Architecture:
  Input(8) → Linear(32) → ReLU → Linear(16) → ReLU → Linear(1) → Sigmoid

Input:  8 features from fl_features.py
Output: single float between 0.0 and 1.0 (goal probability)
"""

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from pathlib import Path

# ── Constants ──────────────────────────────────────────────────────────────────
INPUT_SIZE  = 8     # must match FEATURE_SIZE in fl_features.py
HIDDEN1     = 32
HIDDEN2     = 16
OUTPUT_SIZE = 1
LEARNING_RATE = 0.001

MODEL_PATH = Path(__file__).parent / "global_model.pt"


# ── Model definition ───────────────────────────────────────────────────────────
class GoalPredictorNet(nn.Module):
    """
    Small feedforward neural network for goal probability prediction.

    Why this architecture?
      - Small enough to train quickly on limited match data
      - Deep enough to learn non-linear patterns
      - Sigmoid output gives us a clean 0-1 probability
    """

    def __init__(self):
        super(GoalPredictorNet, self).__init__()

        self.network = nn.Sequential(
            nn.Linear(INPUT_SIZE, HIDDEN1),   # 8 → 32
            nn.ReLU(),
            nn.Dropout(0.2),                  # prevent overfitting
            nn.Linear(HIDDEN1, HIDDEN2),      # 32 → 16
            nn.ReLU(),
            nn.Linear(HIDDEN2, OUTPUT_SIZE),  # 16 → 1
            nn.Sigmoid(),                     # output between 0 and 1
        )

    def forward(self, x):
        return self.network(x)


# ── Model utilities ────────────────────────────────────────────────────────────
def create_model() -> GoalPredictorNet:
    """Create a fresh untrained model."""
    return GoalPredictorNet()


def get_weights(model: GoalPredictorNet) -> dict:
    """
    Extract model weights as a serializable dict.
    Used by FL clients to send weights to the server.
    """
    return {
        key: value.cpu().numpy().tolist()
        for key, value in model.state_dict().items()
    }


def set_weights(model: GoalPredictorNet, weights: dict) -> GoalPredictorNet:
    """
    Load weights into a model from a dict.
    Used by FL server to apply aggregated weights.
    """
    state_dict = {
        key: torch.tensor(value)
        for key, value in weights.items()
    }
    model.load_state_dict(state_dict)
    return model


def save_model(model: GoalPredictorNet, path: Path = MODEL_PATH):
    """Save model weights to disk."""
    torch.save(model.state_dict(), path)
    print(f"💾 Model saved to {path}")


def load_model(path: Path = MODEL_PATH) -> GoalPredictorNet:
    """Load model weights from disk. Creates fresh model if file not found."""
    model = create_model()
    if path.exists():
        model.load_state_dict(torch.load(path, map_location="cpu"))
        print(f"✅ Model loaded from {path}")
    else:
        print(f"⚠️  No saved model found at {path}. Using fresh model.")
    return model


def train_step(
    model: GoalPredictorNet,
    features: np.ndarray,
    label: float,
    optimizer: optim.Optimizer,
) -> float:
    """
    Single training step on one event.

    Args:
      model:     the neural network
      features:  numpy array of shape (8,)
      label:     1.0 if goal happened in next 10 min, else 0.0
      optimizer: Adam optimizer

    Returns:
      loss value (float) for logging
    """
    model.train()
    optimizer.zero_grad()

    # Convert to tensors
    x = torch.tensor(features, dtype=torch.float32).unsqueeze(0)  # shape: (1, 8)
    y = torch.tensor([[label]], dtype=torch.float32)               # shape: (1, 1)

    # Forward pass
    prediction = model(x)

    # Binary cross entropy loss (good for 0/1 classification)
    loss_fn = nn.BCELoss()
    loss = loss_fn(prediction, y)

    # Backward pass
    loss.backward()
    optimizer.step()

    return loss.item()


def predict(model: GoalPredictorNet, features: np.ndarray) -> float:
    """
    Run inference on a feature vector.

    Returns:
      float between 0.0 and 1.0 representing goal probability
    """
    model.eval()
    with torch.no_grad():
        x = torch.tensor(features, dtype=torch.float32).unsqueeze(0)
        output = model(x)
        return float(output.squeeze())


if __name__ == "__main__":
    # Quick test — create model, run a forward pass
    model = create_model()
    print(f"Model architecture:\n{model}")

    # Dummy input
    dummy_features = np.random.rand(INPUT_SIZE).astype(np.float32)
    prob = predict(model, dummy_features)
    print(f"\nDummy prediction: {prob:.4f} (goal probability)")

    # Test weight extraction
    weights = get_weights(model)
    print(f"\nWeight keys: {list(weights.keys())}")
    print("✅ Model test passed!")
