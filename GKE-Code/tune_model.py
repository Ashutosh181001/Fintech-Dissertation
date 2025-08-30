"""
Model Tuning Module
Uses Optuna to find optimal Isolation Forest parameters
"""

import pandas as pd
import numpy as np
import joblib
import optuna
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import cross_val_score
from sklearn.metrics import make_scorer, f1_score
import logging
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ModelTuner:
    """Tunes Isolation Forest parameters using Optuna"""

    def __init__(self, trades_csv: str = None):
        self.trades_csv = trades_csv or config.STORAGE_CONFIG["trades_csv"]
        self.X_train = None
        self.y_train = None

    def load_training_data(self, n_samples: int = 10000):
        """Load and prepare training data from trades CSV"""
        try:
            # Load trades
            df = pd.read_csv(self.trades_csv)

            # Take last n_samples
            if len(df) > n_samples:
                df = df.tail(n_samples)

            logger.info(f"Loaded {len(df)} trades for tuning")

            # Extract features
            features = []
            labels = []

            for _, row in df.iterrows():
                # Use same features as detection pipeline
                feature_vector = [
                    row.get('z_score', 0),
                    row.get('price_change_pct', 0),
                    row.get('time_gap_sec', 0)
                ]
                features.append(feature_vector)

                # Label based on z-score threshold (for validation)
                # This is a proxy for anomalies
                is_anomaly = abs(row.get('z_score', 0)) > config.ANOMALY_CONFIG["z_score_threshold"]
                labels.append(1 if is_anomaly else 0)

            self.X_train = np.array(features)
            self.y_train = np.array(labels)

            # Print class distribution
            anomaly_rate = np.mean(self.y_train)
            logger.info(f"Anomaly rate in training data: {anomaly_rate:.2%}")

            return True

        except Exception as e:
            logger.error(f"Failed to load training data: {e}")
            return False

    def objective(self, trial):
        """Optuna objective function"""

        # Suggest hyperparameters
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 300),
            'max_samples': trial.suggest_float('max_samples', 0.1, 1.0),
            'contamination': trial.suggest_float('contamination', 0.001, 0.05),
            'max_features': trial.suggest_float('max_features', 0.5, 1.0),
            'bootstrap': trial.suggest_categorical('bootstrap', [True, False]),
            'random_state': 42
        }

        # Create model
        model = IsolationForest(**params)

        # For Isolation Forest, we can't use standard CV with labels
        # Instead, we'll use a custom scoring based on anomaly detection performance
        try:
            # Fit model
            model.fit(self.X_train)

            # Get predictions
            predictions = model.predict(self.X_train)
            anomaly_scores = model.decision_function(self.X_train)

            # Convert to binary (1 for normal, -1 for anomaly -> 0 for normal, 1 for anomaly)
            pred_binary = (predictions == -1).astype(int)

            # Calculate metrics
            # We want to maximize detection of high z-score anomalies
            # while minimizing false positives

            # True positives: correctly identified anomalies
            true_positives = np.sum((pred_binary == 1) & (self.y_train == 1))

            # False positives: incorrectly flagged as anomalies
            false_positives = np.sum((pred_binary == 1) & (self.y_train == 0))

            # False negatives: missed anomalies
            false_negatives = np.sum((pred_binary == 0) & (self.y_train == 1))

            # Calculate F1 score
            precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
            recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0

            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

            return f1

        except Exception as e:
            logger.error(f"Error in objective function: {e}")
            return 0.0

    def tune(self, n_trials: int = 100):
        """Run hyperparameter tuning"""

        if self.X_train is None:
            logger.error("No training data loaded. Run load_training_data() first.")
            return None

        logger.info(f"Starting hyperparameter tuning with {n_trials} trials...")

        # Create study
        study = optuna.create_study(
            direction='maximize',
            study_name='isolation_forest_tuning'
        )

        # Optimize
        study.optimize(self.objective, n_trials=n_trials, show_progress_bar=True)

        # Get best parameters
        best_params = study.best_params
        best_score = study.best_value

        logger.info(f"Best F1 score: {best_score:.4f}")
        logger.info(f"Best parameters: {best_params}")

        # Train final model with best parameters
        best_params['random_state'] = 42
        best_model = IsolationForest(**best_params)
        best_model.fit(self.X_train)

        return best_model, best_params, study

    def save_model(self, model, params: dict):
        """Save tuned model and parameters"""
        try:
            # Save model
            joblib.dump(model, config.MODEL_PATHS["tuned_model"])
            logger.info(f"Model saved to {config.MODEL_PATHS['tuned_model']}")

            # Save parameters for reference
            import json
            params_file = config.MODEL_PATHS["tuned_model"].replace('.pkl', '_params.json')
            with open(params_file, 'w') as f:
                json.dump(params, f, indent=2)
            logger.info(f"Parameters saved to {params_file}")

            return True

        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            return False

    def evaluate_model(self, model):
        """Evaluate model performance"""
        if self.X_train is None:
            return None

        # Get predictions
        predictions = model.predict(self.X_train)
        pred_binary = (predictions == -1).astype(int)

        # Calculate metrics
        from sklearn.metrics import classification_report, confusion_matrix

        report = classification_report(
            self.y_train,
            pred_binary,
            target_names=['Normal', 'Anomaly']
        )

        cm = confusion_matrix(self.y_train, pred_binary)

        logger.info("\nClassification Report:")
        logger.info(report)

        logger.info("\nConfusion Matrix:")
        logger.info(f"True Negatives: {cm[0, 0]}")
        logger.info(f"False Positives: {cm[0, 1]}")
        logger.info(f"False Negatives: {cm[1, 0]}")
        logger.info(f"True Positives: {cm[1, 1]}")

        return {
            'classification_report': report,
            'confusion_matrix': cm
        }


def main():
    """Main tuning function"""
    logger.info("=" * 60)
    logger.info("ISOLATION FOREST HYPERPARAMETER TUNING")
    logger.info("=" * 60)

    # Initialize tuner
    tuner = ModelTuner()

    # Load data
    logger.info("\n1. Loading training data...")
    if not tuner.load_training_data(n_samples=10000):
        logger.error("Failed to load training data")
        return

    # Run tuning
    logger.info("\n2. Running hyperparameter optimization...")
    result = tuner.tune(n_trials=100)

    if result is None:
        logger.error("Tuning failed")
        return

    best_model, best_params, study = result

    # Evaluate model
    logger.info("\n3. Evaluating best model...")
    evaluation = tuner.evaluate_model(best_model)

    # Save model
    logger.info("\n4. Saving tuned model...")
    if tuner.save_model(best_model, best_params):
        logger.info("✅ Model tuning complete!")

    # Print optimization history
    logger.info("\n5. Optimization Summary:")
    logger.info(f"Best trial: {study.best_trial.number}")
    logger.info(f"Best value: {study.best_value:.4f}")

    # Print parameter importance
    try:
        importance = optuna.importance.get_param_importances(study)
        logger.info("\nParameter Importance:")
        for param, imp in importance.items():
            logger.info(f"  {param}: {imp:.4f}")
    except:
        pass

    logger.info("\n" + "=" * 60)
    logger.info("Tuning complete! Model saved to: " + config.MODEL_PATHS["tuned_model"])
    logger.info("=" * 60)


if __name__ == "__main__":
    main()