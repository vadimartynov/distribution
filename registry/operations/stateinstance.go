package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/docker/distribution/registry/storage/driver"
)

// File path to store the operations state
const opsStatesFilePath = "/operations-state.json"

var (
	// Ensures that the operations state is only initialized once
	operationsStateInstanceOnce sync.Once
	// Stores the singleton instance of OperationsStateInstance
	operationsStateInstance *OperationsStateInstance
	// Error captured during the initialization of the operations state
	operationsStateInstanceErr error
)

// Structure to hold the mode and operation states
type OperationsStateContent struct {
	Mode           string                 `json:"mode"`
	OperationState map[string]interface{} `json:"operationsState,omitempty"`
}

// Singleton class that manages the operations state
type OperationsStateInstance struct {
	sync.Mutex
	content       OperationsStateContent
	storageDriver driver.StorageDriver
	ctx           context.Context
}

// Returns the singleton instance of OperationsStateInstance, initializing it if necessary
func GetOperationsStateInstance(ctx context.Context, storageDriver driver.StorageDriver, registryMode string) (*OperationsStateInstance, error) {
	// Ensures that the initialization happens only once
	operationsStateInstanceOnce.Do(func() {
		operationsStateInstance, operationsStateInstanceErr = getOperationsStateInstanceOnce(ctx, storageDriver, registryMode)
	})

	// Return the error if initialization failed
	if operationsStateInstanceErr != nil {
		return nil, operationsStateInstanceErr
	}

	// Check if the instance mode matches the requested registry mode
	if operationsStateInstance.content.Mode != registryMode {
		return nil, fmt.Errorf("current instance has mode '%s', required '%s'", operationsStateInstance.content.Mode, registryMode)
	}
	return operationsStateInstance, nil
}

// Initializes and loads the OperationsStateInstance
func getOperationsStateInstanceOnce(ctx context.Context, storageDriver driver.StorageDriver, registryMode string) (*OperationsStateInstance, error) {
	getInstance := func() (*OperationsStateInstance, error) {
		instance := &OperationsStateInstance{
			content: OperationsStateContent{
				Mode:           registryMode,
				OperationState: make(map[string]interface{}),
			},
			storageDriver: storageDriver,
			ctx:           ctx,
		}

		// Check if the file exists, if not, return a new instance
		if _, err := storageDriver.Stat(ctx, opsStatesFilePath); err != nil {
			if _, ok := err.(driver.PathNotFoundError); ok {
				return instance, nil
			}
			return nil, err
		}

		// Load the content from the file
		bytes, err := storageDriver.GetContent(ctx, opsStatesFilePath)
		if err != nil {
			return nil, err
		}

		// Unmarshal the content into the OperationsStateContent struct
		var content OperationsStateContent
		if err := json.Unmarshal(bytes, &content); err != nil {
			return nil, err
		}

		// If the file's mode matches the registry mode, use its content
		if content.Mode == registryMode {
			instance.content = content
		}
		return instance, nil
	}

	instance, err := getInstance()
	if err != nil {
		return nil, err
	}
	return instance, instance.SaveOperationsState()
}

// Sets an operation state by name and saves the updated state
func (i *OperationsStateInstance) SetOperationsState(operationName string, operationValue any) error {
	i.Lock() // Lock for thread safety
	defer i.Unlock()

	i.content.OperationState[operationName] = operationValue
	return i.saveOperationsState()
}

// Retrieves an operation state by name
func (i *OperationsStateInstance) GetOperationsState(operationName string) interface{} {
	i.Lock() // Lock for thread safety
	defer i.Unlock()

	operation, exists := i.content.OperationState[operationName]
	if !exists {
		return nil
	}
	return operation
}

// Saves the current state of operations
func (i *OperationsStateInstance) SaveOperationsState() error {
	i.Lock() // Lock for thread safety
	defer i.Unlock()

	return i.saveOperationsState()
}

// Internal helper function to save the state to storage
func (i *OperationsStateInstance) saveOperationsState() error {
	// Marshal the content to JSON format
	jsonBytes, err := json.Marshal(i.content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %w", err)
	}

	// Save the marshaled content to the file
	if err = i.storageDriver.PutContent(i.ctx, opsStatesFilePath, jsonBytes); err != nil {
		return fmt.Errorf("failed to save content: %w", err)
	}
	return nil
}
